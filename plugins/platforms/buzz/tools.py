"""Owner-gated Buzz work routing for a primary coordinator.

The model selects only human-readable route and agent names.  Channel IDs,
specialist pubkeys, relay credentials, and the signing key remain host-owned
configuration.  The schema is gated only by process-stable configuration;
the handler rechecks the exact Buzz home channel and owner on every call.
"""

from __future__ import annotations

import hashlib
import json
import re
import threading
import time
from typing import Any, Mapping

from gateway.session_context import get_session_env

_HEX_PUBKEY = re.compile(r"^[0-9a-f]{64}$")
_CHANNEL_ID = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_MAX_TITLE_CHARS = 80
_MAX_TASK_CHARS = 48_000
_MAX_AGENTS = 3
_PENDING_TTL_SECONDS = 120
_STATE_LOCK = threading.RLock()


def _runtime_config() -> dict:
    try:
        from hermes_cli.config import load_config_readonly

        config = load_config_readonly() or {}
        return config if isinstance(config, dict) else {}
    except Exception:
        return {}


def _buzz_extra(config: Mapping[str, Any] | None = None) -> dict:
    config = config or _runtime_config()
    gateway = config.get("gateway") if isinstance(config, Mapping) else None
    platforms = gateway.get("platforms") if isinstance(gateway, Mapping) else None
    buzz = platforms.get("buzz") if isinstance(platforms, Mapping) else None
    if not isinstance(buzz, Mapping):
        return {}
    extra = buzz.get("extra", buzz)
    return dict(extra) if isinstance(extra, Mapping) else {}


def _orchestration_config(config: Mapping[str, Any] | None = None) -> dict:
    raw = _buzz_extra(config).get("orchestration")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _normalize_pubkey(value: Any) -> str:
    text = str(value or "").strip().lower()
    if _HEX_PUBKEY.fullmatch(text):
        return text
    try:
        from .adapter import _normalize_user_ref

        return _normalize_user_ref(text) or ""
    except Exception:
        return ""


def _orchestration_configured(config: Mapping[str, Any] | None = None) -> bool:
    """Return whether the host supplied a minimally valid static config.

    Registry ``check_fn`` results are cached process-wide, so this function
    must never inspect per-session context. Call authorization belongs in
    ``_configured_owner`` below and is evaluated for every invocation.
    """
    orchestration = _orchestration_config(config)
    if orchestration.get("enabled") is not True:
        return False
    home_channel = str(orchestration.get("home_channel") or "").strip()
    if not _CHANNEL_ID.fullmatch(home_channel):
        return False
    raw_allowed = orchestration.get("allowed_users")
    if not isinstance(raw_allowed, (list, tuple, set)):
        return False
    # Keep the registry probe cheap and adapter-independent. Exact pubkey/npub
    # validation runs in the per-call authorization path below.
    allowed = {str(value).strip() for value in raw_allowed if str(value).strip()}
    routes = orchestration.get("routes")
    return bool(allowed and isinstance(routes, Mapping) and routes)


def _configured_owner(config: Mapping[str, Any] | None = None) -> bool:
    if not _orchestration_configured(config):
        return False
    orchestration = _orchestration_config(config)
    if get_session_env("HERMES_SESSION_PLATFORM", "").strip().lower() != "buzz":
        return False
    home_channel = str(orchestration.get("home_channel") or "").strip().lower()
    if get_session_env("HERMES_SESSION_CHAT_ID", "").strip().lower() != home_channel:
        return False
    caller = _normalize_pubkey(get_session_env("HERMES_SESSION_USER_ID", ""))
    allowed = {
        normalized
        for value in orchestration.get("allowed_users") or []
        if (normalized := _normalize_pubkey(value))
    }
    return bool(caller and caller in allowed)


def _route(config: Mapping[str, Any], route_name: str) -> tuple[dict | None, str]:
    orchestration = _orchestration_config(config)
    routes = orchestration.get("routes")
    if not isinstance(routes, Mapping):
        return None, "No Buzz orchestration routes are configured."
    route = routes.get(route_name)
    if not isinstance(route, Mapping):
        choices = ", ".join(sorted(str(name) for name in routes)) or "none"
        return None, f"Unknown route '{route_name}'. Configured routes: {choices}."
    return dict(route), ""


def _configured_agents(
    orchestration: Mapping[str, Any], route: Mapping[str, Any]
) -> tuple[dict[str, tuple[str, str]], str]:
    global_agents = orchestration.get("agents")
    route_agents = route.get("agents")
    if isinstance(route_agents, Mapping):
        configured = route_agents
    elif isinstance(global_agents, Mapping) and isinstance(route_agents, list):
        configured = {
            str(name): global_agents.get(str(name))
            for name in route_agents
            if str(name) in global_agents
        }
    else:
        configured = None
    if not isinstance(configured, Mapping):
        return {}, "This route has no specialist allow-list."

    by_name = {
        str(name).strip().lower(): (str(name).strip(), value)
        for name, value in configured.items()
    }
    resolved: dict[str, tuple[str, str]] = {}
    for key, (display_name, raw_pubkey) in by_name.items():
        pubkey = _normalize_pubkey(raw_pubkey)
        if not pubkey:
            return {}, f"Specialist '{display_name}' has an invalid configured pubkey."
        resolved[key] = (display_name, pubkey)
    return resolved, ""


def _assignments(
    orchestration: Mapping[str, Any], route: Mapping[str, Any], args: Mapping[str, Any]
) -> tuple[list[tuple[str, str, str, str]], str]:
    configured, error = _configured_agents(orchestration, route)
    if error:
        return [], error

    legacy_agents = args.get("agents")
    requested_agent = args.get("agent")
    requested_assignments = args.get("assignments")
    selected_inputs = sum(
        value is not None for value in (legacy_agents, requested_agent, requested_assignments)
    )
    if selected_inputs > 1:
        return [], "Choose one assignment form: agent or structured assignments."

    raw_assignments: list[tuple[Any, str, str]] = []
    if legacy_agents is not None:
        if not isinstance(legacy_agents, list) or not legacy_agents:
            return [], "Choose a specialist or use the route primary."
        if len(legacy_agents) > 1:
            return [], (
                "Multi-agent work requires a distinct responsibility and acceptance "
                "gate for every specialist; use structured assignments."
            )
        raw_assignments.append((legacy_agents[0], "", ""))
    elif requested_agent is not None:
        raw_assignments.append((requested_agent, "", ""))
    elif requested_assignments is not None:
        if not isinstance(requested_assignments, list) or not requested_assignments:
            return [], "Structured assignments must contain at least one specialist."
        if len(requested_assignments) > _MAX_AGENTS:
            return [], f"Choose at most {_MAX_AGENTS} specialists."
        for item in requested_assignments:
            if not isinstance(item, Mapping):
                return [], "Every structured assignment must be an object."
            responsibility = str(item.get("responsibility") or "").strip()
            acceptance = str(item.get("acceptance") or "").strip()
            if not responsibility or not acceptance:
                return [], (
                    "Every structured assignment requires a non-empty responsibility "
                    "and acceptance gate."
                )
            raw_assignments.append((item.get("agent"), responsibility, acceptance))
    else:
        primary = route.get("primary_agent") or orchestration.get("primary_agent")
        if not str(primary or "").strip():
            return [], "This route has no configured primary specialist."
        raw_assignments.append((primary, "", ""))

    resolved: list[tuple[str, str, str, str]] = []
    seen: set[str] = set()
    for raw_name, responsibility, acceptance in raw_assignments:
        key = str(raw_name or "").strip().lower()
        entry = configured.get(key)
        if entry is None:
            choices = ", ".join(sorted(name for name, _ in configured.values())) or "none"
            return [], (
                f"Specialist '{raw_name}' is not allowed for this route. Allowed: {choices}."
            )
        display_name, pubkey = entry
        if pubkey in seen:
            return [], f"Specialist '{display_name}' is assigned more than once."
        seen.add(pubkey)
        resolved.append((display_name, pubkey, responsibility, acceptance))
    return resolved, ""


def _validate_text(args: Mapping[str, Any]) -> tuple[str, str, str]:
    title = " ".join(str(args.get("title") or "").split())
    task = str(args.get("task") or "").strip()
    if not title:
        return "", "", "A work title is required."
    if len(title) > _MAX_TITLE_CHARS:
        return "", "", f"The title exceeds {_MAX_TITLE_CHARS} characters."
    if any(ord(char) < 32 for char in title):
        return "", "", "The title contains control characters."
    if not task:
        return "", "", "A concrete task is required."
    if len(task) > _MAX_TASK_CHARS:
        return "", "", f"The task exceeds {_MAX_TASK_CHARS} characters."
    return title, task, ""


def _state_key(
    source_event: str,
    route_name: str,
    title: str,
    task: str,
    assignments: list[tuple[str, str, str, str]],
) -> str:
    canonical = json.dumps(
        {
            "source": source_event,
            "route": route_name,
            "title": title,
            "task": task,
            "assignments": [
                {
                    "pubkey": pubkey,
                    "responsibility": responsibility,
                    "acceptance": acceptance,
                }
                for _, pubkey, responsibility, acceptance in assignments
            ],
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"orchestration:{digest}"


def _success_text(record: Mapping[str, Any]) -> str:
    agents = ", ".join(str(name) for name in record.get("agents") or [])
    return (
        f"Werkplek aangemaakt in #{record.get('route_label')}: {record.get('title')}\n"
        f"Link: {record.get('link')}\n"
        f"Toegewezen aan: {agents}\n"
        "Deel deze link in je antwoord en zeg dat de specialist in de nieuwe "
        "werkthread semantisch is geadresseerd. Maak dezelfde werkplek niet opnieuw."
    )


async def buzz_orchestrate(args: dict, *, state=None, **_: Any) -> str:
    """Create one idempotent, specialist-addressed Buzz work root."""
    config = _runtime_config()
    if not _configured_owner(config):
        return "Error: buzz_orchestrate is restricted to the configured Buzz owner in the coordinator channel."

    orchestration = _orchestration_config(config)
    route_name = str(args.get("route") or "").strip()
    route, error = _route(config, route_name)
    if error or route is None:
        return f"Error: {error}"
    title, task, error = _validate_text(args)
    if error:
        return f"Error: {error}"
    assignments, error = _assignments(orchestration, route, args)
    if error:
        return f"Error: {error}"

    channel_id = str(route.get("channel_id") or "").strip().lower()
    if not _CHANNEL_ID.fullmatch(channel_id):
        return "Error: the selected route has an invalid configured channel ID."
    route_kind = str(route.get("kind") or "forum").strip().lower()
    if route_kind not in {"forum", "stream"}:
        return "Error: the selected route kind must be 'forum' or 'stream'."
    source_event = get_session_env("HERMES_SESSION_MESSAGE_ID", "").strip().lower()
    source_channel = get_session_env("HERMES_SESSION_CHAT_ID", "").strip().lower()
    if not _HEX_PUBKEY.fullmatch(source_event):
        return "Error: the originating Buzz event ID is unavailable; no work thread was created."
    if not _CHANNEL_ID.fullmatch(source_channel):
        return "Error: the originating Buzz channel is invalid; no work thread was created."

    key = _state_key(source_event, route_name, title, task, assignments)
    now = int(time.time())
    if state is not None:
        with _STATE_LOCK:
            prior = state.get(key, {})
            if isinstance(prior, Mapping):
                status = prior.get("status")
                if status == "complete":
                    return _success_text(prior)
                if status == "indeterminate":
                    return (
                        "Error: an earlier publish attempt had an indeterminate network result. "
                        "Reconcile the target channel before retrying to avoid a duplicate work thread."
                    )
                if (
                    status == "pending"
                    and now - int(prior.get("updated_at") or 0) < _PENDING_TTL_SECONDS
                ):
                    return "Error: this exact orchestration request is already being published."
            state.set(key, {"status": "pending", "updated_at": now})

    route_label = str(route.get("label") or route_name).strip() or route_name
    names = [name for name, _, _, _ in assignments]
    origin_link = f"buzz://message?channel={source_channel}&id={source_event}"
    body = task if route_kind == "forum" else f"# {title}\n\n{task}"
    if len(assignments) == 1:
        name, _, responsibility, acceptance = assignments[0]
        assignment_text = f"Primaire uitvoerder: {name}"
        if responsibility:
            assignment_text += f"\nVerantwoordelijkheid: {responsibility}"
        if acceptance:
            assignment_text += f"\nAcceptatie: {acceptance}"
    else:
        rows = ["Taakverdeling:"]
        for name, _, responsibility, acceptance in assignments:
            rows.extend(
                (
                    f"- {name}",
                    f"  Verantwoordelijkheid: {responsibility}",
                    f"  Acceptatie: {acceptance}",
                )
            )
        assignment_text = "\n".join(rows)
    content = (
        f"{body}\n\n---\nVanuit het coördinatorkanaal georkestreerd.\n"
        f"{assignment_text}\n"
        f"Bron: {origin_link}"
    )

    from .adapter import (
        _cli_error_message,
        _exec_buzz,
        _resolve_cli_path,
        _resolve_private_key,
    )

    extra = _buzz_extra(config)
    relay_url = str(extra.get("relay_url") or "").strip()
    cli_path = _resolve_cli_path(str(extra.get("cli_path") or "").strip())
    private_key = _resolve_private_key(extra)
    if not relay_url or not cli_path or not private_key:
        if state is not None:
            state.set(key, {"status": "failed", "updated_at": int(time.time())})
        return "Error: Buzz relay, CLI, or signing identity is not configured for orchestration."
    try:
        from .nostr_auth import public_key_hex

        coordinator_pubkey = public_key_hex(private_key)
    except (TypeError, ValueError):
        if state is not None:
            state.set(key, {"status": "failed", "updated_at": int(time.time())})
        return "Error: the configured Buzz orchestration signing identity is invalid."

    command = [
        "messages",
        "send",
        "--channel",
        channel_id,
        "--content",
        "-",
        "--kind",
        "45001" if route_kind == "forum" else "9",
    ]
    if route_kind == "forum":
        command.extend(("--title", title))
    command.append("--callback-to-sender")
    for _, pubkey, _, _ in assignments:
        command.extend(("--mention", pubkey))

    code, stdout, stderr = await _exec_buzz(
        cli_path,
        command,
        relay_url=relay_url,
        private_key=private_key,
        input_text=content,
    )
    if code != 0:
        status = "indeterminate" if code in {2, 124} else "failed"
        if state is not None:
            state.set(key, {"status": status, "updated_at": int(time.time())})
        return f"Error: {_cli_error_message(stderr, code)}"

    try:
        result = json.loads(stdout or "{}")
        event_id = str(result.get("event_id") or "").strip().lower()
        accepted = result.get("accepted", True)
        emitted_mentions = [
            _normalize_pubkey(value) for value in result.get("mention_pubkeys") or []
        ]
        callback_pubkey = _normalize_pubkey(result.get("callback_pubkey"))
    except (AttributeError, ValueError):
        event_id, accepted, emitted_mentions, callback_pubkey = "", False, [], ""
    expected_mentions = [pubkey for _, pubkey, _, _ in assignments]
    envelope_matches = (
        len(emitted_mentions) == len(expected_mentions)
        and set(emitted_mentions) == set(expected_mentions)
        and callback_pubkey == coordinator_pubkey
        and coordinator_pubkey not in expected_mentions
    )
    if (
        not _HEX_PUBKEY.fullmatch(event_id)
        or accepted is False
        or not envelope_matches
    ):
        if state is not None:
            state.set(key, {"status": "indeterminate", "updated_at": int(time.time())})
        return (
            "Error: Buzz returned no verifiable signed assignment/callback envelope; "
            "reconcile the target channel before retrying."
        )

    record = {
        "status": "complete",
        "updated_at": int(time.time()),
        "event_id": event_id,
        "link": f"buzz://message?channel={channel_id}&id={event_id}",
        "route_label": route_label,
        "title": title,
        "agents": names,
    }
    if state is not None:
        state.set(key, record)
    return _success_text(record)


BUZZ_ORCHESTRATE_SCHEMA = {
    "name": "buzz_orchestrate",
    "description": (
        "Create one real, titled Buzz work thread from the configured #nabu "
        "coordinator channel and semantically assign one or more allow-listed "
        "specialist agents. Use it for a concrete request that belongs in a "
        "project/research/operations forum; do not use it for casual chat. "
        "The host owns channel IDs, pubkeys, signing credentials, and permission checks."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "route": {
                "type": "string",
                "description": "Configured destination route name selected by subject area.",
            },
            "title": {
                "type": "string",
                "description": "Short specific title for the new work thread (max 80 characters).",
            },
            "task": {
                "type": "string",
                "description": "Self-contained assignment with outcome, constraints, and acceptance evidence.",
            },
            "agent": {
                "type": "string",
                "description": "Optional single specialist; omit to use the route's host-owned primary.",
            },
            "assignments": {
                "type": "array",
                "minItems": 1,
                "maxItems": _MAX_AGENTS,
                "description": "Structured multi-specialist work; every entry owns a distinct responsibility and acceptance gate.",
                "items": {
                    "type": "object",
                    "properties": {
                        "agent": {"type": "string"},
                        "responsibility": {"type": "string", "minLength": 1},
                        "acceptance": {"type": "string", "minLength": 1},
                    },
                    "required": ["agent", "responsibility", "acceptance"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["route", "title", "task"],
        "additionalProperties": False,
    },
}


def register_tools(ctx) -> None:
    state = ctx.state

    async def _handler(args: dict, **kwargs: Any) -> str:
        return await buzz_orchestrate(args, state=state, **kwargs)

    ctx.register_tool(
        name="buzz_orchestrate",
        toolset="buzz",
        schema=BUZZ_ORCHESTRATE_SCHEMA,
        handler=_handler,
        check_fn=_orchestration_configured,
        is_async=True,
        description=BUZZ_ORCHESTRATE_SCHEMA["description"],
        emoji="🧭",
    )
