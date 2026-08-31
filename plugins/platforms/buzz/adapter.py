"""
Buzz Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to a Buzz community relay
(Block's open-source human+agent collaboration platform, built on the
Nostr protocol) and relays messages to/from the Hermes agent.

The adapter does not speak Nostr itself — it shells out to the ``buzz``
CLI binary ("JSON in, JSON out") via ``asyncio.create_subprocess_exec``.
Inbound delivery uses a poll loop (the CLI is request/response); see the
"Known limitations" note in the platform docs.

Configuration in config.yaml::

    gateway:
      platforms:
        buzz:
          enabled: true
          extra:
            relay_url: https://mycommunity.communities.buzz.xyz
            channels:                  # channel UUIDs to watch (empty = all joined)
              - ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd
            home_channel: ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd
            require_mention: true       # home channel is implicitly addressed
            accept_bare_slash_commands: false  # reserve bare slash for a primary agent
            poll_interval: 4           # seconds between poll sweeps
            cli_path: ""               # path to the buzz binary (default: PATH, then ~/bin/buzz)
            credentials_file: ""       # JSON file holding the nsec (fallback for BUZZ_PRIVATE_KEY)
            allowed_users: []          # empty = allow all; entries are hex pubkeys or npubs
            observe_unaddressed_messages: false  # context-only; never dispatches the agent
            channel_skill_bindings:    # auto-load one or more skills per Buzz channel/forum
              - id: ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd
                skills: [research, summarize]
            orchestration:             # owner-only Nabu work routing
              enabled: false
              home_channel: ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd
              allowed_users: []        # exact owner pubkeys; required when enabled
              routes: {}               # configured route + specialist allow-lists
            terminal_enabled: false     # encrypted mobile PTY, disabled by default
            terminal_channel: ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd
            terminal_allowed_users: [] # exact owner pubkeys; required when enabled
            terminal_cwd: ""            # defaults to the Hermes user's home

Or via environment variables (overrides config.yaml):
    BUZZ_RELAY_URL, BUZZ_CHANNELS, BUZZ_HOME_CHANNEL, BUZZ_POLL_INTERVAL,
    BUZZ_CLI_PATH, BUZZ_CREDENTIALS_FILE, BUZZ_ALLOWED_USERS,
    BUZZ_ALLOW_ALL_USERS, BUZZ_REQUIRE_MENTION

The only secret is BUZZ_PRIVATE_KEY (nsec or hex) — it belongs in
``~/.hermes/.env``.  It is passed to the CLI via the subprocess
environment and is never logged.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import time
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

from agent.secret_scope import UnscopedSecretError as _UnscopedSecretError
from agent.secret_scope import get_secret as _scoped_get_secret


def _get_scoped_secret(name, default=None):
    """Scope-aware credential read with the default-profile startup fallback.

    Secondary profiles construct their adapters under a profile secret
    scope -- the scope is authoritative and a scoped miss returns ``default``
    (no cross-profile borrow from ``os.environ``, which may hold another
    profile's value). The DEFAULT profile's adapter constructs and sends
    *unscoped* under multiplexing, where a bare ``get_secret`` would raise
    ``UnscopedSecretError`` and crash this path; there ``os.environ`` is that
    profile's own value, so fall back to it. Same pattern as the Slack
    ``SLACK_APP_TOKEN`` read (#59739) and
    ``gateway/platforms/whatsapp_common.py::_get_wsecret``.
    """
    try:
        val = _scoped_get_secret(name, default)
    except _UnscopedSecretError:
        val = os.getenv(name)
    return val if val is not None else default


def _register_orchestration_runtime(adapter) -> None:
    if not __package__:
        return
    from .tools import register_orchestration_adapter

    register_orchestration_adapter(adapter)


def _unregister_orchestration_runtime(adapter) -> None:
    if not __package__:
        return
    from .tools import unregister_orchestration_adapter

    unregister_orchestration_adapter(adapter)


logger = logging.getLogger(__name__)

_LONG_MESSAGE_FILENAME = "buzz-message.md"
_LONG_MESSAGE_MARKER = "The complete lossless message is attached as buzz-message.md."
_LONG_MESSAGE_MAX_BYTES = 100 * 1024 * 1024

from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    ProcessingOutcome,
    SendResult,
    resolve_channel_skills,
)
from gateway.config import Platform


# Buzz conversation events span the legacy stream kind, the structured stream
# kind, diffs, and the forum post/comment pair. ``buzz messages get`` returns
# exactly this user-visible set alongside no housekeeping kinds. Keep the
# native WebSocket subscription and the shared dispatch gate aligned with the
# CLI or an addressed forum topic can be stored perfectly while the agent never
# sees it.
_STREAM_MESSAGE_KINDS = (9, 40002)
_STREAM_DIFF_KIND = 40008
_FORUM_POST_KIND = 45001
_FORUM_COMMENT_KIND = 45003
_CONVERSATION_KINDS = (
    *_STREAM_MESSAGE_KINDS,
    _STREAM_DIFF_KIND,
    _FORUM_POST_KIND,
    _FORUM_COMMENT_KIND,
)
_FORUM_KINDS = (_FORUM_POST_KIND, _FORUM_COMMENT_KIND)
# How many events to request per poll / seed call.
_FETCH_LIMIT = 50
# Bound on the per-channel de-dupe set (events, not bytes).
_SEEN_CAP = 500
# Re-run DM discovery (``dms list`` plus the channels-list fallback) every
# N poll sweeps to pick up conversations opened mid-run.
_DM_DISCOVERY_EVERY = 5
_CHANNEL_POLICY_TTL_SECONDS = 5.0
_THREAD_POLICY_TTL_SECONDS = 2.0

_DEFAULT_POLL_INTERVAL = 4.0
_MIN_POLL_INTERVAL = 1.0
_CLI_TIMEOUT = 30.0
# Relay presence has a short Redis lease. Refresh well inside that lease so a
# connected Hermes gateway remains visibly available on every Buzz client.
_PRESENCE_HEARTBEAT_INTERVAL = 20.0

# WebSocket transport (NIP-42 authenticated Nostr subscription).
# kind 44100 is Buzz's channel-membership event — used for live DM discovery.
_WS_AUTH_TIMEOUT = 20.0
_WS_MAX_MESSAGE_BYTES = 2_000_000
_WS_MEMBERSHIP_KIND = 44100
_WS_MEMBERSHIP_SUB_ID = "hermes-buzz-membership"
_WS_TERMINAL_KIND = 24200
_WS_TERMINAL_SUB_ID = "hermes-buzz-terminal-control"

# Where to look for a credentials JSON (keys: nsec / private_key_hex) when
# BUZZ_PRIVATE_KEY is not set.  Module-level so tests can point it at a tmpdir.
_DEFAULT_CREDENTIALS_DIR = Path("~/.config/buzz").expanduser()


def _load_nostr_auth():
    """Import the sibling nostr_auth module in a loader-agnostic way.

    The adapter is imported both as a package module
    (``plugins.platforms.buzz.adapter``) and as a bare single-file module by
    the test plugin loader, where relative imports have no parent package.
    """
    try:
        from . import nostr_auth  # type: ignore[no-redef]

        return nostr_auth
    except ImportError:
        import importlib.util

        path = Path(__file__).with_name("nostr_auth.py")
        spec = importlib.util.spec_from_file_location(
            "plugin_adapter_buzz_nostr_auth", path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


def _load_terminal_broker():
    """Import the sibling terminal broker under both plugin loader shapes."""
    try:
        from .terminal_broker import TerminalBroker

        return TerminalBroker
    except ImportError:
        import importlib.util
        import sys

        path = Path(__file__).with_name("terminal_broker.py")
        module_name = "plugin_adapter_buzz_terminal_broker"
        spec = importlib.util.spec_from_file_location(module_name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise
        return module.TerminalBroker


# ---------------------------------------------------------------------------
# bech32 (BIP-173) helpers — used to convert between npub and hex pubkeys so
# mention detection and allow-lists accept either form.  Pure stdlib.
# ---------------------------------------------------------------------------

_BECH32_CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"


def _bech32_polymod(values: List[int]) -> int:
    generator = (0x3B6A57B2, 0x26508E6D, 0x1EA119FA, 0x3D4233DD, 0x2A1462B3)
    chk = 1
    for value in values:
        top = chk >> 25
        chk = (chk & 0x1FFFFFF) << 5 ^ value
        for i in range(5):
            chk ^= generator[i] if ((top >> i) & 1) else 0
    return chk


def _bech32_hrp_expand(hrp: str) -> List[int]:
    return [ord(c) >> 5 for c in hrp] + [0] + [ord(c) & 31 for c in hrp]


def _convertbits(
    data, frombits: int, tobits: int, pad: bool = True
) -> Optional[List[int]]:
    acc = 0
    bits = 0
    ret = []
    maxv = (1 << tobits) - 1
    for value in data:
        if value < 0 or (value >> frombits):
            return None
        acc = (acc << frombits) | value
        bits += frombits
        while bits >= tobits:
            bits -= tobits
            ret.append((acc >> bits) & maxv)
    if pad:
        if bits:
            ret.append((acc << (tobits - bits)) & maxv)
    elif bits >= frombits or ((acc << (tobits - bits)) & maxv):
        return None
    return ret


def hex_to_npub(pubkey_hex: str) -> Optional[str]:
    """Encode a 64-char hex pubkey as an ``npub1…`` bech32 string."""
    try:
        raw = bytes.fromhex(pubkey_hex)
    except ValueError:
        return None
    if len(raw) != 32:
        return None
    data = _convertbits(raw, 8, 5)
    if data is None:
        return None
    values = _bech32_hrp_expand("npub") + data
    polymod = _bech32_polymod(values + [0, 0, 0, 0, 0, 0]) ^ 1
    checksum = [(polymod >> 5 * (5 - i)) & 31 for i in range(6)]
    return "npub1" + "".join(_BECH32_CHARSET[d] for d in data + checksum)


def npub_to_hex(npub: str) -> Optional[str]:
    """Decode an ``npub1…`` bech32 string to a 64-char hex pubkey."""
    npub = npub.strip().lower()
    if not npub.startswith("npub1"):
        return None
    data_part = npub[len("npub1") :]
    try:
        data = [_BECH32_CHARSET.index(c) for c in data_part]
    except ValueError:
        return None
    if _bech32_polymod(_bech32_hrp_expand("npub") + data) != 1:
        return None
    decoded = _convertbits(data[:-6], 5, 8, pad=False)
    if decoded is None or len(decoded) != 32:
        return None
    return bytes(decoded).hex()


def _normalize_user_ref(ref: str) -> Optional[str]:
    """Normalize a user reference (hex pubkey or npub) to lowercase hex."""
    ref = (ref or "").strip().lower()
    if not ref:
        return None
    if ref.startswith("npub1"):
        return npub_to_hex(ref)
    if re.fullmatch(r"[0-9a-f]{64}", ref):
        return ref
    return None


# ---------------------------------------------------------------------------
# buzz-cli invocation helpers
# ---------------------------------------------------------------------------


def _resolve_cli_path(configured: str = "") -> str:
    """Resolve the buzz CLI binary path portably.

    Order: explicit config value → ``buzz`` on PATH → ``~/bin/buzz``.
    Returns "" when nothing is found so callers can raise a config error.
    """
    if configured:
        p = Path(configured).expanduser()
        return str(p) if p.is_file() else ""
    found = shutil.which("buzz")
    if found:
        return found
    fallback = Path.home() / "bin" / "buzz"
    return str(fallback) if fallback.is_file() else ""


def _resolve_private_key(extra: Optional[dict] = None) -> str:
    """Resolve the Nostr private key: env first, then a credentials JSON.

    NEVER log the return value.
    """
    key = _get_scoped_secret("BUZZ_PRIVATE_KEY", "").strip()
    if key:
        return key
    configured = os.getenv("BUZZ_CREDENTIALS_FILE", "").strip() or (extra or {}).get(
        "credentials_file", ""
    )
    if configured:
        candidates = [Path(configured).expanduser()]
    else:
        try:
            candidates = sorted(_DEFAULT_CREDENTIALS_DIR.glob("*credentials*.json"))
        except OSError:
            candidates = []
    for path in candidates:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(data, dict):
            continue
        for field in ("nsec", "private_key_hex", "private_key"):
            value = data.get(field)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


async def _exec_buzz(
    cli_path: str,
    args: List[str],
    *,
    relay_url: str,
    private_key: str,
    input_text: Optional[str] = None,
    timeout: float = _CLI_TIMEOUT,
) -> Tuple[int, str, str]:
    """Run the buzz CLI with an argument list (never a shell) and return
    ``(returncode, stdout, stderr)``.

    The private key travels via the subprocess environment only — it never
    appears in argv, so process listings and error logs stay clean.
    """
    env = os.environ.copy()
    env["BUZZ_RELAY_URL"] = relay_url
    env["BUZZ_PRIVATE_KEY"] = private_key
    proc = await asyncio.create_subprocess_exec(
        cli_path,
        *args,
        stdin=asyncio.subprocess.PIPE
        if input_text is not None
        else asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            proc.communicate(
                input_text.encode("utf-8") if input_text is not None else None
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return (
            124,
            "",
            json.dumps({
                "error": "timeout",
                "message": f"buzz {args[0] if args else ''} timed out after {timeout}s",
            }),
        )
    return (
        proc.returncode if proc.returncode is not None else 4,
        stdout.decode("utf-8", errors="replace"),
        stderr.decode("utf-8", errors="replace"),
    )


async def _exec_buzz_bytes(
    cli_path: str,
    args: List[str],
    *,
    relay_url: str,
    private_key: str,
    timeout: float = _CLI_TIMEOUT,
) -> Tuple[int, bytes, str]:
    """Run one Buzz CLI read that deliberately returns raw bytes on stdout."""
    env = os.environ.copy()
    env["BUZZ_RELAY_URL"] = relay_url
    env["BUZZ_PRIVATE_KEY"] = private_key
    proc = await asyncio.create_subprocess_exec(
        cli_path,
        *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return 124, b"", f"buzz media get timed out after {timeout}s"
    return (
        proc.returncode if proc.returncode is not None else 4,
        stdout,
        stderr.decode("utf-8", errors="replace"),
    )


def _cli_error_message(stderr: str, returncode: int) -> str:
    """Extract the human-readable message from the CLI's JSON error contract.

    stderr is ``{"error": "<category>", "message": "<detail>"}`` on failure;
    fall back to the raw (stripped) stderr when it isn't JSON.
    """
    text = (stderr or "").strip()
    try:
        data = json.loads(text)
        if isinstance(data, dict) and data.get("message"):
            return (
                f"{data.get('error', 'error')}: {data['message']} (exit {returncode})"
            )
    except ValueError:
        pass
    return text or f"buzz CLI failed with exit code {returncode}"


def _parse_json_list(stdout: str) -> List[dict]:
    """Parse CLI stdout expected to be a JSON array of objects."""
    try:
        data = json.loads(stdout or "[]")
    except ValueError:
        return []
    if not isinstance(data, list):
        return []
    return [item for item in data if isinstance(item, dict)]


# ---------------------------------------------------------------------------
# Buzz Adapter
# ---------------------------------------------------------------------------


class BuzzAdapter(BasePlatformAdapter):
    """Poll-based Buzz adapter implementing the BasePlatformAdapter interface.

    Instantiated by the adapter_factory passed to register_platform().
    """

    supports_structured_tool_activity = True
    REQUIRES_EDIT_FINALIZE = True

    def __init__(self, config, **kwargs):
        platform = Platform("buzz")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}
        self._extra = extra

        # Connection settings (env vars override config.yaml)
        self.relay_url = (
            os.getenv("BUZZ_RELAY_URL") or extra.get("relay_url", "")
        ).strip()
        self.cli_path = _resolve_cli_path(
            os.getenv("BUZZ_CLI_PATH", "").strip()
            or str(extra.get("cli_path", "") or "")
        )

        # Channels to watch: env csv > extra list/csv; empty = all joined channels
        raw_channels = os.getenv("BUZZ_CHANNELS") or extra.get("channels", [])
        if isinstance(raw_channels, str):
            raw_channels = raw_channels.split(",")
        self.channels: List[str] = [
            c.strip() for c in raw_channels if isinstance(c, str) and c.strip()
        ]

        self.home_channel = (
            os.getenv("BUZZ_HOME_CHANNEL") or str(extra.get("home_channel", "") or "")
        ).strip()

        try:
            interval = float(
                os.getenv("BUZZ_POLL_INTERVAL")
                or extra.get("poll_interval", _DEFAULT_POLL_INTERVAL)
            )
        except (TypeError, ValueError):
            interval = _DEFAULT_POLL_INTERVAL
        self.poll_interval = max(_MIN_POLL_INTERVAL, interval)

        # Whether channel messages must @mention the agent to get a response.
        # Defaults to True (respond only when addressed). Set False to make the
        # agent respond to every message in a watched channel. DMs always
        # dispatch regardless. Env (BUZZ_REQUIRE_MENTION) overrides config.yaml.
        _rm_raw = os.getenv("BUZZ_REQUIRE_MENTION")
        if _rm_raw is None:
            _rm_cfg = extra.get("require_mention", True)
        else:
            _rm_cfg = _rm_raw
        self.require_mention = str(_rm_cfg).strip().lower() not in (
            "false",
            "0",
            "no",
            "off",
        )

        # A bare `/command` can be reserved for one primary agent (Nabu) while
        # specialist agents still require explicit `@name /command`
        # addressing. Disabled by default so existing multi-agent gateways do
        # not all consume the same unaddressed command.
        _bare_slash_cfg = extra.get("accept_bare_slash_commands", False)
        self.accept_bare_slash_commands = str(_bare_slash_cfg).strip().lower() in (
            "true",
            "1",
            "yes",
            "on",
        )

        # Preserve unaddressed channel/forum traffic as context in the
        # canonical thread session without running the agent or sending a
        # response. This lets a primary coordinator see direct specialist
        # instructions and results when it is addressed later in that thread.
        _observe_cfg = extra.get("observe_unaddressed_messages", False)
        self.observe_unaddressed_messages = str(_observe_cfg).strip().lower() in (
            "true",
            "1",
            "yes",
            "on",
        )

        _terminal_cfg = os.getenv("BUZZ_TERMINAL_ENABLED")
        if _terminal_cfg is None:
            _terminal_cfg = extra.get("terminal_enabled", False)
        self.terminal_enabled = str(_terminal_cfg).strip().lower() in (
            "true",
            "1",
            "yes",
            "on",
        )
        self.terminal_channel = (
            os.getenv("BUZZ_TERMINAL_CHANNEL")
            or str(extra.get("terminal_channel", "") or "")
            or self.home_channel
        ).strip()
        self.terminal_cwd = (
            os.getenv("BUZZ_TERMINAL_CWD") or str(extra.get("terminal_cwd", "") or "")
        ).strip()
        raw_terminal_allowed = os.getenv("BUZZ_TERMINAL_ALLOWED_USERS") or extra.get(
            "terminal_allowed_users", []
        )
        if isinstance(raw_terminal_allowed, str):
            raw_terminal_allowed = raw_terminal_allowed.split(",")
        self._terminal_allowed_pubkeys: set[str] = {
            normalized
            for entry in raw_terminal_allowed
            if isinstance(entry, str) and (normalized := _normalize_user_ref(entry))
        }

        # Inbound transport: "auto" (WebSocket with poll fallback, default),
        # "websocket" (require WS; fail connect when it can't authenticate),
        # or "poll" (CLI polling only). Env (BUZZ_TRANSPORT) overrides
        # config.yaml.
        _transport = (
            (
                os.getenv("BUZZ_TRANSPORT")
                or str(extra.get("transport", "auto") or "auto")
            )
            .strip()
            .lower()
        )
        self.transport = (
            _transport if _transport in ("auto", "websocket", "poll") else "auto"
        )

        # Auth: entries may be hex pubkeys or npubs; normalized to hex
        raw_allowed = os.getenv("BUZZ_ALLOWED_USERS") or extra.get("allowed_users", [])
        if isinstance(raw_allowed, str):
            raw_allowed = raw_allowed.split(",")
        self._allowed_pubkeys: set = {
            normalized
            for entry in raw_allowed
            if isinstance(entry, str) and (normalized := _normalize_user_ref(entry))
        }

        # Secret — resolved lazily (never at import/registration time and
        # never logged).  connect() re-resolves it to fail fast with a clear
        # error when it is missing.
        self._private_key: str = ""

        # Identity — filled in by connect() from ``buzz users get``
        self._self_pubkey: str = ""
        self._self_npub: str = ""
        self._display_name: str = ""

        # Runtime state
        self._poll_task: Optional[asyncio.Task] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._presence_task: Optional[asyncio.Task] = None
        self._typing_publish_tasks: Dict[str, asyncio.Task] = {}
        self._activity_publish_tasks: set[asyncio.Task] = set()
        self._presence_announced = False
        self._ws_ready: Optional[asyncio.Event] = None
        self._ws_active = False  # True while the WS loop owns inbound delivery
        self._membership_since = 0
        self._lock_key: Optional[str] = None
        # channel_id -> {"chat_type", "forum", "last_ts",
        #                "seen": OrderedDict[event_id, None]}
        self._channel_state: Dict[str, dict] = {}
        self._channel_names: Dict[str, str] = {}
        # channel_id -> raw ``channels list`` entry; drives DM-vs-channel
        # classification (see _may_reclassify_as_dm).
        self._channel_meta: Dict[str, dict] = {}
        self._channel_policy_checked_at: Dict[str, float] = {}
        # root event id -> (explicit policy or None for inherit, checked_at)
        self._thread_policy_cache: Dict[str, Tuple[Optional[bool], float]] = {}
        self._user_names: Dict[str, str] = {}
        self._poll_count = 0
        self._terminal_broker = None

    @property
    def name(self) -> str:
        return "Buzz"

    # ── buzz-cli plumbing ─────────────────────────────────────────────────

    async def _run_cli(
        self, args: List[str], *, input_text: Optional[str] = None
    ) -> Tuple[int, str, str]:
        if not self._private_key:
            self._private_key = _resolve_private_key(self._extra)
        return await _exec_buzz(
            self.cli_path,
            args,
            relay_url=self.relay_url,
            private_key=self._private_key,
            input_text=input_text,
        )

    async def _run_cli_bytes(self, args: List[str]) -> Tuple[int, bytes, str]:
        if not self._private_key:
            self._private_key = _resolve_private_key(self._extra)
        return await _exec_buzz_bytes(
            self.cli_path,
            args,
            relay_url=self.relay_url,
            private_key=self._private_key,
        )

    async def _decrypt_terminal_event(self, event: dict) -> dict:
        code, out, err = await self._run_cli(
            ["agents", "observer-decrypt", "--event", "-"],
            input_text=json.dumps(event, separators=(",", ":")),
        )
        if code != 0:
            raise ValueError(_cli_error_message(err, code))
        decoded = json.loads(out or "{}")
        if not isinstance(decoded, dict):
            raise ValueError("buzz observer-decrypt returned a non-object")
        return decoded

    async def _publish_terminal_telemetry(self, payload: dict) -> bool:
        code, _out, err = await self._run_cli(
            ["agents", "observer-telemetry", "--payload", "-"],
            input_text=json.dumps(payload, separators=(",", ":")),
        )
        if code != 0:
            logger.warning(
                "Buzz: terminal telemetry publish failed — %s",
                _cli_error_message(err, code),
            )
            return False
        return True

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Verify relay credentials, seed high-water marks, start polling."""
        if not self.relay_url:
            logger.error("Buzz: relay URL must be configured")
            self._set_fatal_error(
                "config_missing", "BUZZ_RELAY_URL must be set", retryable=False
            )
            return False
        if not self.cli_path:
            logger.error(
                "Buzz: buzz CLI binary not found (set BUZZ_CLI_PATH or put 'buzz' on PATH)"
            )
            self._set_fatal_error(
                "cli_missing", "buzz CLI binary not found", retryable=False
            )
            return False
        self._private_key = _resolve_private_key(self._extra)
        if not self._private_key:
            logger.error(
                "Buzz: no private key (set BUZZ_PRIVATE_KEY or a credentials file)"
            )
            self._set_fatal_error(
                "config_missing", "BUZZ_PRIVATE_KEY must be set", retryable=False
            )
            return False

        # Learn our own identity: pubkey drives self-echo suppression and
        # display name drives channel mention gating.
        code, out, err = await self._run_cli(["users", "get"])
        if code != 0:
            message = _cli_error_message(err, code)
            logger.error(
                "Buzz: failed to fetch own profile from %s — %s",
                self.relay_url,
                message,
            )
            self._set_fatal_error("connect_failed", message, retryable=code == 2)
            return False
        profiles = _parse_json_list(out)
        if not profiles or not profiles[0].get("pubkey"):
            logger.error(
                "Buzz: 'users get' returned no profile — is the key a member of this community?"
            )
            self._set_fatal_error(
                "connect_failed", "buzz users get returned no profile", retryable=True
            )
            return False
        self._self_pubkey = str(profiles[0]["pubkey"]).lower()
        self._display_name = str(profiles[0].get("display_name") or "").strip()
        self._self_npub = hex_to_npub(self._self_pubkey) or ""

        # Prevent two profiles from driving the same Buzz identity on the
        # same relay (duplicate replies, split de-dupe state). Mirrors the
        # IRC adapter's scoped-lock pattern.
        try:
            from gateway.status import acquire_scoped_lock

            lock_key = f"{self.relay_url}:{self._self_pubkey}"
            if not acquire_scoped_lock("buzz", lock_key):
                logger.error(
                    "Buzz: identity %s… on %s already in use by another profile",
                    self._self_pubkey[:8],
                    self.relay_url,
                )
                self._set_fatal_error(
                    "lock_conflict",
                    "Buzz identity in use by another profile",
                    retryable=False,
                )
                return False
            self._lock_key = lock_key
        except ImportError:
            self._lock_key = None  # status module not available (e.g. tests)

        # Map channel ids to names and pick the watch set.
        code, out, err = await self._run_cli(["channels", "list"])
        if code != 0:
            message = _cli_error_message(err, code)
            logger.error("Buzz: failed to list channels — %s", message)
            self._set_fatal_error("connect_failed", message, retryable=code == 2)
            return False
        listed = _parse_json_list(out)
        self._channel_names = {
            str(ch.get("channel_id")): str(ch.get("name") or ch.get("channel_id"))
            for ch in listed
            if ch.get("channel_id")
        }
        for ch in listed:
            if ch.get("channel_id"):
                self._channel_meta[str(ch["channel_id"])] = ch
        if self.terminal_enabled:
            if self.transport == "poll":
                self._set_fatal_error(
                    "config_invalid",
                    "Buzz terminal requires websocket or auto transport",
                    retryable=False,
                )
                return False
            if not self.terminal_channel or not self._terminal_allowed_pubkeys:
                self._set_fatal_error(
                    "config_invalid",
                    "Buzz terminal requires terminal_channel and terminal_allowed_users",
                    retryable=False,
                )
                return False
            if self.terminal_channel not in self._channel_names:
                self._set_fatal_error(
                    "config_invalid",
                    "Buzz terminal_channel must be a joined channel",
                    retryable=False,
                )
                return False
        watch = self.channels or list(self._channel_names)
        if not watch:
            logger.error(
                "Buzz: no channels to watch (configure BUZZ_CHANNELS or join a channel)"
            )
            self._set_fatal_error(
                "config_missing", "no Buzz channels to watch", retryable=False
            )
            return False

        # Seed high-water marks from the newest events so a (re)start never
        # replays channel history into the agent.
        for channel_id in watch:
            await self._seed_channel(channel_id, chat_type="group")
        await self._discover_dms(seed=True)

        if self.terminal_enabled:
            try:
                broker_type = _load_terminal_broker()
                self._terminal_broker = broker_type(
                    agent_pubkey=self._self_pubkey,
                    allowed_users=self._terminal_allowed_pubkeys,
                    channel_id=self.terminal_channel,
                    cwd=self.terminal_cwd,
                    decrypt_event=self._decrypt_terminal_event,
                    publish_telemetry=self._publish_terminal_telemetry,
                )
                await self._terminal_broker.start()
            except Exception as exc:
                self._set_fatal_error(
                    "terminal_start_failed", str(exc), retryable=False
                )
                await self.disconnect()
                return False

        # Inbound transport: prefer the NIP-42-authenticated WebSocket
        # subscription (push, near-zero latency); fall back to CLI polling
        # when the WS can't be established (transport="auto") or when the
        # user pinned transport="poll".
        transport_used = "poll"
        if self.transport in ("auto", "websocket"):
            if await self._start_websocket():
                transport_used = "websocket"
            elif self.transport == "websocket" or self.terminal_enabled:
                self._set_fatal_error(
                    "ws_auth_failed",
                    "Buzz WebSocket transport did not authenticate (transport=websocket)",
                    retryable=True,
                )
                await self.disconnect()
                return False
        if transport_used == "poll":
            self._poll_task = asyncio.create_task(self._poll_loop())
        await self._publish_presence("online")
        self._presence_task = asyncio.create_task(self._presence_loop())
        self._mark_connected()
        _register_orchestration_runtime(self)
        logger.info(
            "Buzz: connected to %s as %s, watching %d channel(s) via %s%s",
            self.relay_url,
            self._display_name or self._self_npub[:16],
            len(self._channel_state),
            transport_used,
            ""
            if transport_used == "websocket"
            else f", poll interval {self.poll_interval:.1f}s",
        )
        return True

    async def disconnect(self) -> None:
        """Stop the inbound transport and drop runtime state."""
        _unregister_orchestration_runtime(self)
        self._mark_disconnected()
        typing_publish_tasks = list(getattr(self, "_typing_publish_tasks", {}).values())
        for task in typing_publish_tasks:
            if not task.done():
                task.cancel()
        if typing_publish_tasks:
            await asyncio.gather(*typing_publish_tasks, return_exceptions=True)
        self._typing_publish_tasks = {}
        activity_publish_tasks = list(getattr(self, "_activity_publish_tasks", set()))
        for task in activity_publish_tasks:
            if not task.done():
                task.cancel()
        if activity_publish_tasks:
            await asyncio.gather(*activity_publish_tasks, return_exceptions=True)
        self._activity_publish_tasks = set()
        if self._presence_task and not self._presence_task.done():
            self._presence_task.cancel()
            try:
                await self._presence_task
            except asyncio.CancelledError:
                pass
        self._presence_task = None
        if self._presence_announced:
            await self._publish_presence("offline")
        lock_key = getattr(self, "_lock_key", None)
        if lock_key:
            try:
                from gateway.status import release_scoped_lock

                release_scoped_lock("buzz", lock_key)
            except Exception:
                pass
            self._lock_key = None
        self._ws_active = False
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        self._ws_task = None
        if self._terminal_broker is not None:
            await self._terminal_broker.stop()
            self._terminal_broker = None
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
        self._poll_task = None
        self._channel_state = {}
        self._poll_count = 0

    async def _publish_presence(self, status: str) -> bool:
        """Publish one authenticated ephemeral presence update."""
        code, _out, err = await self._run_cli([
            "users",
            "set-presence",
            "--status",
            status,
        ])
        if code != 0:
            logger.warning(
                "Buzz: failed to publish %s presence — %s",
                status,
                _cli_error_message(err, code),
            )
            return False
        self._presence_announced = status != "offline"
        return True

    async def _presence_loop(self) -> None:
        """Renew the relay's presence lease until the adapter disconnects."""
        try:
            while True:
                await asyncio.sleep(_PRESENCE_HEARTBEAT_INTERVAL)
                await self._publish_presence("online")
        except asyncio.CancelledError:
            raise

    # ── Sending ───────────────────────────────────────────────────────────

    def prefers_fresh_final_streaming(
        self, content: str, metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Commit completed streamed replies as fresh immutable messages."""
        return True

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not content:
            return SendResult(success=False, error="Empty message")
        args = ["messages", "send", "--channel", str(chat_id), "--content", "-"]
        # Explicit Buzz threads stay one level deep and always reply to their
        # canonical outer root. Ordinary channel and DM turns deliberately
        # stay top-level: the gateway still supplies the triggering message as
        # ``reply_to``, but turning that into a NIP-10 reply would silently
        # manufacture a thread for every prompt.
        thread_id = (metadata or {}).get("thread_id")
        if (metadata or {}).get("notify") is True and not (metadata or {}).get(
            "_interim_send"
        ):
            args.append("--final-response")
        reply_target = thread_id
        if reply_target:
            args += ["--reply-to", str(reply_target)]
            if self._channel_state.get(str(chat_id), {}).get("forum"):
                args += ["--kind", str(_FORUM_COMMENT_KIND)]
        code, out, err = await self._run_cli(args, input_text=content)
        if code != 0:
            return SendResult(
                success=False,
                error=_cli_error_message(err, code),
                retryable=code == 2,
            )
        try:
            data = json.loads(out or "{}")
        except ValueError:
            data = {}
        event_id = data.get("event_id")
        if event_id:
            # Belt-and-braces echo suppression: the poll loop already skips
            # our own pubkey, but marking the id seen makes de-dupe explicit.
            self._mark_seen(str(chat_id), str(event_id))
        return SendResult(
            success=bool(data.get("accepted", True)),
            message_id=str(event_id) if event_id else None,
            raw_response=data,
        )

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Edit one Buzz message for coalesced live response streaming.

        Content travels over stdin so partial model output never enters the
        process argument list. ``finalize`` and routing metadata are no-ops:
        Buzz edits retain the target message's channel/thread relationship.
        """
        del chat_id, finalize, metadata
        if not content:
            return SendResult(success=False, error="Empty message")
        code, out, err = await self._run_cli(
            ["messages", "edit", "--event", str(message_id), "--content", "-"],
            input_text=content,
        )
        if code != 0:
            return SendResult(
                success=False,
                error=_cli_error_message(err, code),
                retryable=code == 2,
            )
        try:
            data = json.loads(out or "{}")
        except ValueError:
            data = {}
        event_id = data.get("event_id")
        return SendResult(
            success=bool(data.get("accepted", True)),
            message_id=str(message_id),
            raw_response={**data, "edit_event_id": event_id},
        )

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Publish a Buzz deletion event for one streamed preview message."""
        if not message_id:
            return False
        code, out, err = await self._run_cli([
            "messages",
            "delete",
            "--event",
            str(message_id),
        ])
        if code != 0:
            logger.debug(
                "Buzz: failed to delete streamed preview %s — %s",
                message_id,
                _cli_error_message(err, code),
            )
            return False
        try:
            data = json.loads(out or "{}")
        except ValueError:
            return False
        if not bool(data.get("accepted")):
            return False
        event_id = data.get("event_id")
        if event_id:
            self._mark_seen(str(chat_id), str(event_id))
        return True

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Queue a short-lived Buzz typing heartbeat without blocking cadence."""
        if not self.cli_path:
            return
        thread_id = str((metadata or {}).get("thread_id") or "")
        route_key = f"{chat_id}:{thread_id}"
        tasks = getattr(self, "_typing_publish_tasks", None)
        if tasks is None:
            tasks = {}
            self._typing_publish_tasks = tasks
        existing = tasks.get(route_key)
        if existing is not None and not existing.done():
            return

        async def _publish() -> None:
            args = ["messages", "typing", "--channel", str(chat_id)]
            if thread_id:
                args += ["--thread", thread_id]
            try:
                code, _out, err = await self._run_cli(args)
            except Exception as exc:
                logger.debug("Buzz: typing heartbeat failed for %s — %s", chat_id, exc)
                return
            if code != 0:
                logger.debug(
                    "Buzz: typing heartbeat failed for %s — %s",
                    chat_id,
                    _cli_error_message(err, code),
                )

        task = asyncio.create_task(_publish())
        tasks[route_key] = task
        task.add_done_callback(
            lambda finished, key=route_key: (
                tasks.pop(key, None) if tasks.get(key) is finished else None
            )
        )

    async def stop_typing(self, chat_id: str, metadata=None) -> None:
        """Typing is a renewable Buzz lease; stopping stops its refresh."""

    async def _publish_activity(
        self,
        *,
        chat_id: str,
        kind: str,
        payload: dict,
        session_id: Optional[str] = None,
        turn_id: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        args = [
            "agents",
            "activity",
            "--channel",
            str(chat_id),
            "--kind",
            kind,
            "--payload",
            "-",
        ]
        # Preserve Hermes' session identifier for transcript correlation while
        # sending the Buzz thread root as its own scope. Channel-root activity
        # deliberately omits --thread so clients never have to infer scope from
        # an internal session id.
        resolved_thread = str((metadata or {}).get("thread_id") or "")
        resolved_session = resolved_thread or session_id
        if resolved_session:
            args += ["--session", resolved_session]
        if resolved_thread:
            args += ["--thread", resolved_thread]
        if turn_id:
            args += ["--turn", str(turn_id)]
        try:
            code, _out, err = await self._run_cli(
                args,
                input_text=json.dumps(payload, separators=(",", ":")),
            )
        except Exception as exc:
            logger.debug(
                "Buzz: structured activity publish failed for %s — %s",
                chat_id,
                exc,
            )
            return
        if code != 0:
            logger.debug(
                "Buzz: structured activity publish failed for %s — %s",
                chat_id,
                _cli_error_message(err, code),
            )

    def _queue_activity(self, **kwargs) -> None:
        """Publish best-effort activity without delaying the chat response."""
        task = asyncio.create_task(self._publish_activity(**kwargs))
        tasks = getattr(self, "_activity_publish_tasks", None)
        if tasks is None:
            tasks = set()
            self._activity_publish_tasks = tasks
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async def publish_tool_started(
        self,
        chat_id: str,
        tool_call_id: str,
        tool_name: str,
        args: Optional[Dict[str, Any]] = None,
        *,
        session_id: Optional[str] = None,
        turn_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self._publish_activity(
            chat_id=chat_id,
            kind="acp_read",
            session_id=session_id,
            turn_id=turn_id,
            metadata=metadata,
            payload={
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call",
                        "toolCallId": tool_call_id,
                        "title": tool_name,
                        "toolName": tool_name,
                        "status": "executing",
                        "args": args or {},
                    }
                },
            },
        )

    async def publish_tool_completed(
        self,
        chat_id: str,
        tool_call_id: str,
        tool_name: str,
        *,
        is_error: bool = False,
        session_id: Optional[str] = None,
        turn_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        await self._publish_activity(
            chat_id=chat_id,
            kind="acp_read",
            session_id=session_id,
            turn_id=turn_id,
            metadata=metadata,
            payload={
                "method": "session/update",
                "params": {
                    "update": {
                        "sessionUpdate": "tool_call_update",
                        "toolCallId": tool_call_id,
                        "toolName": tool_name,
                        "status": "failed" if is_error else "completed",
                    }
                },
            },
        )

    async def on_processing_start(self, event: MessageEvent) -> None:
        source = event.source
        turn_id = str(event.message_id or source.message_id or "")
        self._queue_activity(
            chat_id=source.chat_id,
            kind="turn_started",
            session_id=source.thread_id or turn_id,
            turn_id=turn_id or None,
            metadata={"thread_id": source.thread_id} if source.thread_id else None,
            payload={"type": "turn_started"},
        )

    async def on_processing_complete(
        self,
        event: MessageEvent,
        outcome: ProcessingOutcome,
    ) -> None:
        await super().on_processing_complete(event, outcome)
        source = event.source
        turn_id = str(event.message_id or source.message_id or "")
        kind = (
            "turn_completed" if outcome is ProcessingOutcome.SUCCESS else "turn_error"
        )
        self._queue_activity(
            chat_id=source.chat_id,
            kind=kind,
            session_id=source.thread_id or turn_id,
            turn_id=turn_id or None,
            metadata={"thread_id": source.thread_id} if source.thread_id else None,
            payload={"type": kind},
        )

    async def send_reaction(self, chat_id: str, message_id: str, emoji: str) -> bool:
        """Add a reaction to a message via buzz-cli.

        Returns True on success, False on failure. Errors are logged but not
        raised — reactions are best-effort and should never block the main
        message flow.
        """
        if not self.cli_path or not emoji or not message_id:
            return False
        # buzz-cli: `reactions add --event <64-char hex event id> --emoji <e>`.
        # The event id IS the message_id we recorded on dispatch; channel is
        # not a parameter to this subcommand.
        args = [
            "reactions",
            "add",
            "--event",
            str(message_id),
            "--emoji",
            emoji,
        ]
        code, _out, err = await self._run_cli(args)
        if code != 0:
            logger.debug(
                "Buzz: reaction add failed for message %s in %s — %s",
                message_id[:12],
                chat_id,
                _cli_error_message(err, code),
            )
            return False
        return True

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image: local files upload via --file, URLs go as a link."""
        local = (
            Path(image_url).expanduser()
            if not image_url.startswith(("http://", "https://"))
            else None
        )
        if local is not None and local.is_file():
            args = [
                "messages",
                "send",
                "--channel",
                str(chat_id),
                "--file",
                str(local),
                "--content",
                "-",
            ]
            thread_id = (metadata or {}).get("thread_id")
            reply_target = thread_id
            if reply_target:
                args += ["--reply-to", str(reply_target)]
                if self._channel_state.get(str(chat_id), {}).get("forum"):
                    args += ["--kind", str(_FORUM_COMMENT_KIND)]
            code, out, err = await self._run_cli(args, input_text=caption or "")
            if code != 0:
                return SendResult(
                    success=False,
                    error=_cli_error_message(err, code),
                    retryable=code == 2,
                )
            try:
                data = json.loads(out or "{}")
            except ValueError:
                data = {}
            event_id = data.get("event_id")
            if event_id:
                self._mark_seen(str(chat_id), str(event_id))
            return SendResult(
                success=bool(data.get("accepted", True)),
                message_id=str(event_id) if event_id else None,
                raw_response=data,
            )
        # Markdown renders in Buzz, so a URL arrives as a clickable image link.
        text = f"{caption}\n{image_url}" if caption else image_url
        return await self.send(chat_id, text, reply_to=reply_to, metadata=metadata)

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        chat_id = str(chat_id)
        state = self._channel_state.get(chat_id)
        chat_type = state["chat_type"] if state else "group"
        name = self._channel_names.get(chat_id)
        if name is None and self.cli_path:
            code, out, _err = await self._run_cli([
                "channels",
                "get",
                "--channel",
                chat_id,
            ])
            if code == 0:
                try:
                    data = json.loads(out or "{}")
                    if isinstance(data, dict) and data.get("name"):
                        name = str(data["name"])
                        self._channel_names[chat_id] = name
                except ValueError:
                    pass
        return {"name": name or chat_id, "type": chat_type, "chat_id": chat_id}

    # ── Inbound: WebSocket transport (NIP-42 authenticated) ──────────────
    #
    # Push transport contributed in PR #73636 by @ScaleLeanChris, adapted to
    # dispatch through the same _handle_event() machinery as the poll loop so
    # de-dupe, mention gating, DM latching, and the allow-list behave
    # identically on both transports.

    def _websocket_url(self) -> str:
        parsed = urlsplit(self.relay_url.strip())
        scheme = {"http": "ws", "https": "wss"}.get(parsed.scheme, parsed.scheme)
        if scheme not in ("ws", "wss") or not parsed.netloc:
            raise ValueError("Buzz relay URL must use http(s) or ws(s)")
        return urlunsplit((scheme, parsed.netloc, parsed.path or "", parsed.query, ""))

    async def _start_websocket(self) -> bool:
        """Start the WS loop; True when it authenticates within the timeout."""
        try:
            import websockets  # noqa: F401  (availability probe)

            self._websocket_url()
        except Exception as e:
            logger.info(
                "Buzz: WebSocket transport unavailable (%s); falling back to polling", e
            )
            return False
        self._ws_ready = asyncio.Event()
        self._membership_since = int(time.time())
        self._ws_task = asyncio.create_task(self._websocket_loop())
        try:
            await asyncio.wait_for(self._ws_ready.wait(), timeout=_WS_AUTH_TIMEOUT + 5)
        except (asyncio.TimeoutError, TimeoutError):
            logger.warning("Buzz: WebSocket did not authenticate in time")
            self._ws_active = False
            if self._ws_task and not self._ws_task.done():
                self._ws_task.cancel()
                try:
                    await self._ws_task
                except asyncio.CancelledError:
                    pass
            self._ws_task = None
            return False
        return True

    async def _authenticate_websocket(self, websocket) -> None:
        """NIP-42: wait for the relay's AUTH challenge, answer with a signed
        kind-22242 event (plus the optional NIP-OA owner-attestation tag from
        BUZZ_AUTH_TAG), and wait for the OK acknowledgment."""
        build_auth_event = _load_nostr_auth().build_auth_event

        raw = await asyncio.wait_for(websocket.recv(), timeout=_WS_AUTH_TIMEOUT)
        message = json.loads(raw)
        if not isinstance(message, list) or len(message) < 2 or message[0] != "AUTH":
            raise ConnectionError("Buzz relay did not send a NIP-42 AUTH challenge")
        event = build_auth_event(
            private_key=self._private_key,
            challenge=str(message[1]),
            relay_url=self._websocket_url(),
            auth_tag_json=os.getenv("BUZZ_AUTH_TAG", ""),
        )
        await websocket.send(json.dumps(["AUTH", event], separators=(",", ":")))
        while True:
            raw = await asyncio.wait_for(websocket.recv(), timeout=_WS_AUTH_TIMEOUT)
            response = json.loads(raw)
            if not isinstance(response, list) or not response:
                continue
            if (
                response[0] == "OK"
                and len(response) >= 4
                and response[1] == event["id"]
            ):
                if response[2] is True:
                    return
                raise ConnectionError(f"Buzz WebSocket AUTH rejected: {response[3]}")
            if response[0] in ("NOTICE", "CLOSED"):
                detail = response[-1] if len(response) > 1 else "authentication failed"
                raise ConnectionError(f"Buzz WebSocket AUTH failed: {detail}")

    async def _send_channel_subscription(
        self, websocket, subscription_id: str, channel_id: str
    ) -> None:
        state = self._channel_state.get(channel_id) or {}
        since = max(int(state.get("last_ts") or time.time()) - 1, 0)
        request = [
            "REQ",
            subscription_id,
            {"kinds": list(_CONVERSATION_KINDS), "#h": [channel_id], "since": since},
        ]
        await websocket.send(json.dumps(request, separators=(",", ":")))

    async def _subscribe_websocket(self, websocket) -> Dict[str, Optional[str]]:
        """Subscribe to every watched conversation plus membership events
        (kind 44100 p-tagged to us) for live DM discovery."""
        subscriptions: Dict[str, Optional[str]] = {}
        for index, channel_id in enumerate(list(self._channel_state)):
            subscription_id = f"hermes-buzz-{index}"
            subscriptions[subscription_id] = channel_id
            await self._send_channel_subscription(
                websocket, subscription_id, channel_id
            )
        if self._self_pubkey:
            request = [
                "REQ",
                _WS_MEMBERSHIP_SUB_ID,
                {
                    "kinds": [_WS_MEMBERSHIP_KIND],
                    "#p": [self._self_pubkey],
                    "since": max(self._membership_since - 1, 0),
                },
            ]
            await websocket.send(json.dumps(request, separators=(",", ":")))
            subscriptions[_WS_MEMBERSHIP_SUB_ID] = None
        if self._terminal_broker is not None and self._self_pubkey:
            request = [
                "REQ",
                _WS_TERMINAL_SUB_ID,
                {
                    "kinds": [_WS_TERMINAL_KIND],
                    "#p": [self._self_pubkey],
                    "#agent": [self._self_pubkey],
                    "#frame": ["control"],
                    "since": max(int(time.time()) - 1, 0),
                },
            ]
            await websocket.send(json.dumps(request, separators=(",", ":")))
            subscriptions[_WS_TERMINAL_SUB_ID] = None
        return subscriptions

    async def _handle_membership_event(
        self, websocket, subscriptions: Dict[str, Optional[str]], event: dict
    ) -> None:
        """A membership event p-tagged to us: rediscover conversations and
        subscribe to any new ones (fresh DMs dispatch from their beginning)."""
        self._membership_since = max(
            self._membership_since, int(event.get("created_at") or 0)
        )
        before = set(self._channel_state)
        await self._discover_dms(seed=False)
        for channel_id in self._channel_state:
            if channel_id in before:
                continue
            subscription_id = f"hermes-buzz-dm-{len(subscriptions)}"
            subscriptions[subscription_id] = channel_id
            await self._send_channel_subscription(
                websocket, subscription_id, channel_id
            )
            logger.info("Buzz: subscribed to new conversation %s", channel_id)

    async def _websocket_loop(self) -> None:
        """Persistent authenticated subscription with bounded reconnect
        backoff. Events route through _handle_event() — identical semantics
        to the poll loop. On reconnect, per-channel `since` filters resume
        from the last observed timestamps (same-second overlap de-duped by
        event id)."""
        import websockets

        backoff = 1.0
        try:
            while True:
                try:
                    async with websockets.connect(
                        self._websocket_url(),
                        open_timeout=_WS_AUTH_TIMEOUT,
                        close_timeout=5,
                        ping_interval=20,
                        ping_timeout=20,
                        max_size=_WS_MAX_MESSAGE_BYTES,
                    ) as websocket:
                        await self._authenticate_websocket(websocket)
                        subscriptions = await self._subscribe_websocket(websocket)
                        self._ws_active = True
                        if self._ws_ready is not None:
                            self._ws_ready.set()
                        backoff = 1.0
                        async for raw in websocket:
                            try:
                                message = json.loads(raw)
                            except (ValueError, TypeError):
                                logger.warning(
                                    "Buzz: ignoring malformed WebSocket frame"
                                )
                                continue
                            if not isinstance(message, list) or not message:
                                continue
                            if message[0] == "EVENT" and len(message) >= 3:
                                subscription_id = str(message[1])
                                event = message[2]
                                if not isinstance(event, dict):
                                    continue
                                if subscription_id == _WS_MEMBERSHIP_SUB_ID:
                                    await self._handle_membership_event(
                                        websocket, subscriptions, event
                                    )
                                    continue
                                if subscription_id == _WS_TERMINAL_SUB_ID:
                                    if self._terminal_broker is not None:
                                        await self._terminal_broker.handle_event(event)
                                    continue
                                channel_id = subscriptions.get(subscription_id)
                                state = self._channel_state.get(channel_id or "")
                                if channel_id and state is not None:
                                    await self._handle_event(channel_id, state, event)
                                    self._trim_seen(state)
                            elif message[0] == "CLOSED":
                                subscription_id = (
                                    str(message[1]) if len(message) > 1 else ""
                                )
                                detail = (
                                    message[-1]
                                    if len(message) > 2
                                    else "subscription closed"
                                )
                                channel_id = subscriptions.pop(subscription_id, None)
                                if subscription_id == _WS_TERMINAL_SUB_ID:
                                    raise ConnectionError(
                                        f"terminal control subscription closed: {detail}"
                                    )
                                if channel_id:
                                    # One stale or revoked channel must not tear
                                    # down every healthy subscription. Drop it
                                    # for this adapter lifetime; a membership
                                    # event can discover it again after access
                                    # is restored.
                                    self._channel_state.pop(channel_id, None)
                                    logger.warning(
                                        "Buzz: channel subscription %s closed; disabling %s: %s",
                                        subscription_id,
                                        channel_id,
                                        detail,
                                    )
                                    continue
                                if subscription_id == _WS_MEMBERSHIP_SUB_ID:
                                    logger.warning(
                                        "Buzz: membership subscription closed: %s",
                                        detail,
                                    )
                                    continue
                                raise ConnectionError(str(detail))
                            elif message[0] == "NOTICE":
                                logger.warning("Buzz: relay notice: %s", message[-1])
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    self._ws_active = False
                    logger.warning(
                        "Buzz: WebSocket disconnected; retrying in %.1fs: %s",
                        backoff,
                        e,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)
        finally:
            self._ws_active = False

    # ── Inbound polling ───────────────────────────────────────────────────

    async def _poll_loop(self) -> None:
        """Poll every watched channel for new events until cancelled."""
        try:
            while True:
                await asyncio.sleep(self.poll_interval)
                self._poll_count += 1
                try:
                    if self._poll_count % _DM_DISCOVERY_EVERY == 0:
                        await self._discover_dms(seed=False)
                    for channel_id in list(self._channel_state):
                        await self._poll_channel(channel_id)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.warning("Buzz: poll sweep failed", exc_info=True)
        except asyncio.CancelledError:
            raise

    async def _seed_channel(self, channel_id: str, chat_type: str) -> None:
        """Initialize a channel's high-water mark from its newest events."""
        state = {
            "chat_type": chat_type,
            "forum": False,
            "last_ts": 0,
            "seen": OrderedDict(),
        }
        self._channel_state[channel_id] = state
        code, out, err = await self._run_cli([
            "messages",
            "get",
            "--channel",
            channel_id,
            "--limit",
            str(_FETCH_LIMIT),
        ])
        if code != 0:
            logger.warning(
                "Buzz: could not seed channel %s — %s",
                channel_id,
                _cli_error_message(err, code),
            )
            # Fall back to "now" so a transiently unreadable channel does not
            # replay its whole history once it becomes readable.
            state["last_ts"] = int(time.time())
            return
        for event in _parse_json_list(out):
            event_id = event.get("id")
            created_at = int(event.get("created_at") or 0)
            if event_id:
                state["seen"][str(event_id)] = None
            state["last_ts"] = max(state["last_ts"], created_at)
            self._note_channel_kind(state, event)
            # History is never dispatched, but it still classifies: a DM that
            # leaked in via ``channels list`` latches to chat_type="dm" here,
            # so it bypasses the mention gate from the very first poll.
            self._maybe_latch_dm(channel_id, state, event)
        self._trim_seen(state)

    async def _discover_dms(self, *, seed: bool) -> None:
        """Watch DM conversations.  New ones found mid-run dispatch from their
        beginning (a fresh conversation has no history worth suppressing);
        ones present at startup are seeded like channels.

        ``dms list`` is only a best-effort source: on some hosted relays it
        returns ``[]`` even when DM conversations exist (#68871).  Those DMs
        DO surface in ``channels list`` as entries named "DM" with an empty
        description, so that listing is scanned as a fallback.  Fallback
        finds are watched as ``group`` and latch to ``dm`` via p-tag
        detection (_is_direct_message_event) rather than trusting the name
        alone to unlock the mention-free DM path.
        """
        code, out, _err = await self._run_cli(["dms", "list"])
        if code == 0:
            for dm in _parse_json_list(out):
                dm_id = str(dm.get("dm_id") or "")
                if not dm_id or dm_id in self._channel_state:
                    continue
                if seed:
                    await self._seed_channel(dm_id, chat_type="dm")
                else:
                    self._channel_state[dm_id] = {
                        "chat_type": "dm",
                        "last_ts": 0,
                        "seen": OrderedDict(),
                    }
                self._channel_names.setdefault(dm_id, "DM")

        code, out, _err = await self._run_cli(["channels", "list"])
        if code != 0:
            return
        for ch in _parse_json_list(out):
            ch_id = str(ch.get("channel_id") or "")
            if not ch_id:
                continue
            self._channel_meta[ch_id] = ch
            self._channel_names.setdefault(ch_id, str(ch.get("name") or ch_id))
            if ch_id in self._channel_state:
                continue
            # With no explicit BUZZ_CHANNELS list the adapter owns all joined
            # conversations, including real channels/forums created after
            # startup. With an explicit list, retain the DM fallback without
            # silently widening the configured channel scope.
            if self.channels and not self._may_reclassify_as_dm(ch_id):
                continue
            if seed:
                await self._seed_channel(ch_id, chat_type="group")
            else:
                self._channel_state[ch_id] = {
                    "chat_type": "group",
                    "forum": False,
                    "last_ts": 0,
                    "seen": OrderedDict(),
                }

    async def _poll_channel(self, channel_id: str) -> None:
        state = self._channel_state.get(channel_id)
        if state is None:
            return
        args = [
            "messages",
            "get",
            "--channel",
            channel_id,
            "--limit",
            str(_FETCH_LIMIT),
        ]
        if state["last_ts"]:
            # Nostr `since` is inclusive: same-second events are re-fetched
            # and de-duped by id below.
            args += ["--since", str(state["last_ts"])]
        code, out, err = await self._run_cli(args)
        if code != 0:
            logger.debug(
                "Buzz: poll of channel %s failed — %s",
                channel_id,
                _cli_error_message(err, code),
            )
            return
        for event in _parse_json_list(out):
            await self._handle_event(channel_id, state, event)
        self._trim_seen(state)

    async def _handle_event(self, channel_id: str, state: dict, event: dict) -> None:
        """De-dupe, filter, and dispatch a single ``messages get`` event."""
        event_id = str(event.get("id") or "")
        created_at = int(event.get("created_at") or 0)
        if not event_id or event_id in state["seen"]:
            return
        state["seen"][event_id] = None
        state["last_ts"] = max(state["last_ts"], created_at)

        event_kind = int(event.get("kind") or 0)
        if event_kind not in _CONVERSATION_KINDS:
            return
        self._note_channel_kind(state, event)
        pubkey = str(event.get("pubkey") or "").lower()
        content = event.get("content")
        if not pubkey or not isinstance(content, str) or not content.strip():
            return

        content = await self._hydrate_long_message_attachment(event, content)

        # Suppress self-echo: never dispatch our own messages back to the agent.
        if pubkey == self._self_pubkey:
            return

        # Reclassify a leaked DM before gating so its first un-mentioned
        # message both latches the conversation and dispatches.
        self._maybe_latch_dm(channel_id, state, event)

        is_dm = state["chat_type"] == "dm"
        # In shared channels, the relay-authored channel metadata and optional
        # thread-root override decide whether an @mention is required. Legacy
        # config remains the fallback for older relays. DMs always dispatch.
        if not is_dm:
            is_bare_slash = content.lstrip().startswith("/")
            mention_required = await self._effective_mention_required(
                channel_id, event
            )
            semantically_targeted = self._has_self_p_target(event)
            explicitly_targeted_elsewhere = self._has_foreign_only_p_targets(event)
            implicitly_addressed = (
                not mention_required and not explicitly_targeted_elsewhere
            ) or semantically_targeted or (
                self.accept_bare_slash_commands and is_bare_slash
            )
            if not implicitly_addressed and not self._is_mentioned(content):
                if self.observe_unaddressed_messages:
                    await self._observe_unaddressed_message(
                        channel_id=channel_id,
                        pubkey=pubkey,
                        content=content,
                        event=event,
                        created_at=created_at,
                    )
                return

        # Adapter-level allow-list (the gateway applies BUZZ_ALLOWED_USERS /
        # BUZZ_ALLOW_ALL_USERS centrally as well; empty list = no filter here).
        if self._allowed_pubkeys and pubkey not in self._allowed_pubkeys:
            # The primary agent's home channel is implicitly addressed, so a
            # specialist result without a p-tag reaches this allow-list rather
            # than the mention gate above. Preserve it as non-dispatching
            # coordination context when observation is enabled. This mirrors
            # unaddressed traffic outside the home channel and never grants the
            # sender authority to invoke the agent.
            if not is_dm and self.observe_unaddressed_messages:
                await self._observe_unaddressed_message(
                    channel_id=channel_id,
                    pubkey=pubkey,
                    content=content,
                    event=event,
                    created_at=created_at,
                )
            logger.debug(
                "Buzz: ignoring message from unauthorized pubkey %s…", pubkey[:8]
            )
            return

        # Strip a leading @mention so slash commands (@Chip /whoami ->
        # /whoami) and clean prompts are recognized. DM messages often still
        # open with "@Chip" even though no mention is required there, so the
        # strip applies to both chat types.
        thread_id = None if is_dm else self._thread_root_id(event)
        quoted_event_ids = self._quoted_event_ids(event)
        if quoted_event_ids:
            selected, missing = await self._load_quoted_events(channel_id, event)
            if missing:
                await self.send(
                    channel_id,
                    "I could not start this turn because some selected Buzz context "
                    "could not be loaded and verified. No partial context was sent "
                    "to the agent; retry after the source messages are available.",
                    metadata={"thread_id": thread_id} if thread_id else None,
                )
                return
            seed_error = await self._seed_selected_context(
                channel_id=channel_id,
                event=event,
                thread_id=thread_id,
                selected=selected,
            )
            if seed_error:
                await self.send(
                    channel_id,
                    f"I could not start this turn with its selected context: {seed_error}",
                    metadata={"thread_id": thread_id} if thread_id else None,
                )
                return

        dispatch_text = self._strip_mention(content)

        await self._dispatch_message(
            text=dispatch_text,
            chat_id=channel_id,
            chat_type="dm" if is_dm else "group",
            user_id=pubkey,
            user_name=await self._resolve_user_name(pubkey),
            message_id=event_id,
            created_at=created_at,
            thread_id=thread_id,
            raw_event=event,
        )

    async def _hydrate_long_message_attachment(self, event: dict, content: str) -> str:
        """Replace Buzz's frame-safe marker with its verified Markdown source.

        Android moves drafts above the relay's inline websocket budget to a
        content-addressed same-origin attachment. Only that exact marker and
        filename opt into hydration. The URL must remain on this relay's media
        origin, the declared size is bounded by the relay's generic-file cap,
        and both size and SHA-256 are verified before any text reaches Hermes.
        """
        if not content.startswith(_LONG_MESSAGE_MARKER):
            return content
        tags = event.get("tags")
        if not isinstance(tags, list):
            return content
        attachment = None
        for tag in tags:
            if not isinstance(tag, (list, tuple)) or len(tag) < 2 or tag[0] != "imeta":
                continue
            fields = {}
            for raw in tag[1:]:
                if not isinstance(raw, str) or " " not in raw:
                    continue
                name, value = raw.split(" ", 1)
                fields[name] = value
            if fields.get("filename") == _LONG_MESSAGE_FILENAME:
                attachment = fields
                break
        if attachment is None:
            return content

        url = attachment.get("url", "")
        digest = attachment.get("x", "").lower()
        try:
            declared_size = int(attachment.get("size", "0"))
        except (TypeError, ValueError):
            return content
        relay = urlsplit(self.relay_url)
        target = urlsplit(url)
        if (
            target.scheme not in ("http", "https")
            or target.hostname != relay.hostname
            or target.port != relay.port
            or not target.path.startswith("/media/")
            or target.query
            or target.fragment
            or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)
            or declared_size <= 0
            or declared_size > _LONG_MESSAGE_MAX_BYTES
        ):
            return content

        code, payload, _stderr = await self._run_cli_bytes([
            "media",
            "get",
            url,
            "--output",
            "-",
        ])
        if code != 0 or len(payload) > _LONG_MESSAGE_MAX_BYTES:
            logger.warning(
                "Buzz: authenticated long-message download failed for %s",
                event.get("id"),
            )
            return content

        def verify() -> Optional[str]:
            if (
                len(payload) != declared_size
                or hashlib.sha256(payload).hexdigest() != digest
            ):
                return None
            try:
                hydrated = payload.decode("utf-8")
            except UnicodeDecodeError:
                return None
            return hydrated if hydrated.strip() else None

        hydrated = await asyncio.to_thread(verify)
        if hydrated is None:
            logger.warning(
                "Buzz: could not verify long-message attachment for %s", event.get("id")
            )
            return content
        return hydrated

    @staticmethod
    def _thread_root_id(event: dict) -> Optional[str]:
        """Return one stable Buzz conversation id from NIP-10 tags.

        Top-level stream messages have no thread id and share the channel
        session. A forum post is itself the durable topic root. Direct replies
        may carry only a reply marker; nested replies carry explicit root +
        reply markers. The outer root always wins, so every message in an
        explicitly created stream thread or forum topic continues one session.
        """
        tags = event.get("tags")
        if isinstance(tags, list):
            for tag in tags:
                if (
                    isinstance(tag, (list, tuple))
                    and len(tag) > 3
                    and tag[0] == "e"
                    and tag[3] == "root"
                    and tag[1]
                ):
                    return str(tag[1])
        is_titled_root = isinstance(tags, list) and any(
            isinstance(tag, (list, tuple))
            and len(tag) > 1
            and tag[0] == "subject"
            for tag in tags
        )
        if (
            int(event.get("kind") or 0) == _FORUM_POST_KIND
            or (int(event.get("kind") or 0) != 40003 and is_titled_root)
        ):
            event_id = str(event.get("id") or "")
            if len(event_id) == 64 and all(
                c in "0123456789abcdef" for c in event_id.lower()
            ):
                return event_id
        if isinstance(tags, list):
            for tag in tags:
                if (
                    isinstance(tag, (list, tuple))
                    and len(tag) > 1
                    and tag[0] == "e"
                    and tag[1]
                ):
                    return str(tag[1])
        return None

    @staticmethod
    def _agent_mention_policy(
        event: dict, agent_pubkey: str = ""
    ) -> Optional[bool]:
        """Return this agent's explicit policy, or None to inherit.

        A two-field tag defines the location default. Three-field tags define
        stable pubkey-specific overrides and take precedence for that agent.
        Malformed or duplicate applicable policy fails closed.
        """
        tags = event.get("tags")
        if not isinstance(tags, list):
            return None
        default_policy: Optional[bool] = None
        default_found = False
        target_policy: Optional[bool] = None
        target_found = False
        normalized_agent = agent_pubkey.strip().lower()
        for tag in tags:
            if not isinstance(tag, (list, tuple)) or not tag:
                continue
            if tag[0] != "agent_mentions":
                continue
            # Thread roots are ordinary message events rather than privileged
            # channel metadata. Treat malformed or duplicate policy tags as
            # mention-required instead of accidentally inheriting an optional
            # channel policy.
            if len(tag) == 2:
                if default_found or tag[1] not in {"required", "optional"}:
                    return True
                default_found = True
                default_policy = tag[1] == "required"
                continue
            if len(tag) != 3 or not isinstance(tag[1], str):
                return True
            target = tag[1].strip().lower()
            if len(target) != 64 or any(c not in "0123456789abcdef" for c in target):
                return True
            if target != normalized_agent:
                continue
            if target_found or tag[2] not in {"required", "optional", "inherit"}:
                return True
            target_found = True
            if tag[2] == "inherit":
                target_policy = None
            else:
                target_policy = tag[2] == "required"
        return target_policy if target_found and target_policy is not None else default_policy

    async def _refresh_channel_policy(self, channel_id: str) -> Optional[bool]:
        now = time.monotonic()
        checked_at = self._channel_policy_checked_at.get(channel_id, 0.0)
        meta = self._channel_meta.get(channel_id)
        if now - checked_at >= _CHANNEL_POLICY_TTL_SECONDS:
            code, out, _err = await self._run_cli(
                ["channels", "get", "--channel", channel_id]
            )
            if code == 0:
                try:
                    loaded = json.loads(out or "null")
                except ValueError:
                    loaded = None
                if isinstance(loaded, dict):
                    meta = loaded
                    self._channel_meta[channel_id] = loaded
                    if loaded.get("name"):
                        self._channel_names[channel_id] = str(loaded["name"])
            self._channel_policy_checked_at[channel_id] = now
        if not isinstance(meta, dict):
            return None
        overrides = meta.get("agent_mention_overrides")
        if isinstance(overrides, dict) and self._self_pubkey:
            value = overrides.get(self._self_pubkey.lower())
            if value == "required":
                return True
            if value == "optional":
                return False
        value = meta.get("agent_mentions")
        if value == "required":
            return True
        if value == "optional":
            return False
        return None

    async def _thread_mention_policy(
        self, channel_id: str, root_id: str, event: dict
    ) -> Optional[bool]:
        # A newly published root is already authoritative and avoids a relay
        # round trip on the first message.
        if str(event.get("id") or "") == root_id:
            explicit = self._agent_mention_policy(event, self._self_pubkey)
            self._thread_policy_cache[root_id] = (explicit, time.monotonic())
            return explicit

        cached = self._thread_policy_cache.get(root_id)
        now = time.monotonic()
        if cached is not None and now - cached[1] < _THREAD_POLICY_TTL_SECONDS:
            return cached[0]

        code, out, _err = await self._run_cli(
            [
                "messages",
                "thread",
                "--channel",
                channel_id,
                "--event",
                root_id,
                "--root-only",
            ]
        )
        explicit: Optional[bool] = None
        if code == 0:
            candidates = sorted(
                _parse_json_list(out), key=lambda item: int(item.get("created_at") or 0)
            )
            for candidate in candidates:
                candidate_id = str(candidate.get("id") or "")
                kind = int(candidate.get("kind") or 0)
                if candidate_id == root_id or kind == 40003:
                    # Message edits are full tag snapshots. An edit without the
                    # tag intentionally restores channel inheritance.
                    explicit = self._agent_mention_policy(candidate, self._self_pubkey)
        self._thread_policy_cache[root_id] = (explicit, now)
        return explicit

    async def _effective_mention_required(
        self, channel_id: str, event: dict
    ) -> bool:
        root_id = self._thread_root_id(event)
        if root_id:
            thread_policy = await self._thread_mention_policy(
                channel_id, root_id, event
            )
            if thread_policy is not None:
                return thread_policy

        channel_policy = await self._refresh_channel_policy(channel_id)
        if channel_policy is not None:
            return channel_policy

        # Backwards compatibility for relays that predate policy metadata.
        if self.home_channel and channel_id == self.home_channel:
            return False
        return self.require_mention

    @staticmethod
    def _note_channel_kind(state: dict, event: dict) -> None:
        """Latch forum routing after observing any forum post/comment.

        ``channels list`` currently does not expose the stream/forum type to
        the adapter. The signed event kind is authoritative and lets outbound
        replies use kind 45003 without inventing deployment-only metadata.
        """
        if int(event.get("kind") or 0) in _FORUM_KINDS:
            state["forum"] = True

    @staticmethod
    def _quoted_event_ids(event: dict) -> list[str]:
        """Return de-duplicated NIP-18 quote ids in selection order."""
        tags = event.get("tags")
        if not isinstance(tags, list):
            return []
        result: list[str] = []
        seen: set[str] = set()
        for tag in tags:
            if not isinstance(tag, (list, tuple)) or len(tag) < 2 or tag[0] != "q":
                continue
            event_id = str(tag[1]).lower()
            if len(event_id) != 64 or any(
                char not in "0123456789abcdef" for char in event_id
            ):
                continue
            if event_id not in seen:
                seen.add(event_id)
                result.append(event_id)
        return result

    async def _load_quoted_events(
        self, channel_id: str, event: dict
    ) -> tuple[list[dict], list[str]]:
        """Load every selected event exactly, preserving selection order.

        Android adds standard NIP-18 ``q`` tags to the first and subsequent
        messages in a user-created thread. ``messages get --event`` performs an
        exact ID lookup plus channel-boundary verification; using
        ``messages thread --limit 1`` here used to omit selected replies in a
        busy thread. Fetches run in small batches so selection count does not
        create unbounded process concurrency.
        """
        event_ids = self._quoted_event_ids(event)
        if not event_ids:
            return [], []

        async def fetch(event_id: str) -> Optional[dict]:
            code, out, _err = await self._run_cli([
                "messages",
                "get",
                "--channel",
                str(channel_id),
                "--event",
                event_id,
            ])
            if code != 0:
                return None
            for candidate in _parse_json_list(out):
                if str(candidate.get("id") or "").lower() == event_id:
                    body = candidate.get("content")
                    if not isinstance(body, str) or not body.strip():
                        return None
                    hydrated = await self._hydrate_long_message_attachment(
                        candidate, body
                    )
                    if body.startswith(_LONG_MESSAGE_MARKER) and hydrated == body:
                        return None
                    return {**candidate, "content": hydrated}
            return None

        resolved: dict[str, dict] = {}
        for offset in range(0, len(event_ids), 8):
            batch = await asyncio.gather(
                *(fetch(event_id) for event_id in event_ids[offset : offset + 8])
            )
            for candidate in batch:
                if candidate is not None:
                    resolved[str(candidate.get("id") or "").lower()] = candidate

        missing = [event_id for event_id in event_ids if event_id not in resolved]
        return [resolved[event_id] for event_id in event_ids if event_id in resolved], missing

    async def _seed_selected_context(
        self,
        *,
        channel_id: str,
        event: dict,
        thread_id: Optional[str],
        selected: list[dict],
    ) -> str:
        """Persist selected context before the first turn in its thread session.

        The context becomes prior observed transcript state instead of being
        flattened into the current user prompt. That gives context engines such
        as LCM-X the same stable session/history input as ordinary earlier
        messages. Source-event markers make replay after reconnect idempotent.
        """
        if not selected:
            return "no selected messages were resolved"
        if not thread_id:
            return "the selected messages are not attached to a canonical thread root"
        store = getattr(self, "_session_store", None)
        if store is None:
            return "the session store is unavailable"

        event_id = str(event.get("id") or "")
        pubkey = str(event.get("pubkey") or "").lower()
        try:
            source = self.build_source(
                chat_id=channel_id,
                chat_name=self._channel_names.get(channel_id, channel_id),
                chat_type="group",
                user_id=pubkey,
                user_name=await self._resolve_user_name(pubkey),
                thread_id=thread_id,
                message_id=event_id,
            )
            session_entry = store.get_or_create_session(source)
            transcript = store.load_transcript(session_entry.session_id)
        except Exception as exc:
            logger.warning("Buzz: selected-context session bootstrap failed: %s", exc)
            return "the fresh thread session could not be initialized"

        already_seeded: set[str] = set()
        for message in transcript or []:
            metadata = message.get("display_metadata")
            if not isinstance(metadata, dict):
                continue
            if metadata.get("selected_context_root") != thread_id:
                continue
            source_event_id = str(metadata.get("source_event_id") or "").lower()
            if source_event_id:
                already_seeded.add(source_event_id)

        total = len(selected)
        try:
            for index, candidate in enumerate(selected, start=1):
                source_event_id = str(candidate.get("id") or "").lower()
                if source_event_id in already_seeded:
                    continue
                author_pubkey = str(candidate.get("pubkey") or "").lower()
                author_name = await self._resolve_user_name(author_pubkey)
                body = str(candidate.get("content") or "")
                structured = json.dumps(
                    {
                        "type": "selected_buzz_context",
                        "index": index,
                        "total": total,
                        "event_id": source_event_id,
                        "author": {
                            "name": author_name or author_pubkey,
                            "pubkey": author_pubkey,
                        },
                        "content": body,
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                created_at = int(candidate.get("created_at") or 0)
                store.append_to_transcript(
                    session_entry.session_id,
                    {
                        "role": "user",
                        "content": structured,
                        "timestamp": (
                            datetime.fromtimestamp(created_at).isoformat()
                            if created_at
                            else datetime.now().isoformat()
                        ),
                        "platform_message_id": source_event_id,
                        "observed": True,
                        "display_kind": "selected_context",
                        "display_metadata": {
                            "selected_context_root": thread_id,
                            "source_event_id": source_event_id,
                            "index": index,
                            "total": total,
                        },
                    },
                )
        except Exception as exc:
            logger.warning("Buzz: selected-context transcript seed failed: %s", exc)
            return "the selected messages could not be persisted losslessly"
        return ""

    async def _observe_unaddressed_message(
        self,
        *,
        channel_id: str,
        pubkey: str,
        content: str,
        event: dict,
        created_at: int,
    ) -> None:
        """Append context-only Buzz traffic to its canonical thread session."""
        store = getattr(self, "_session_store", None)
        if not store:
            return
        try:
            user_name = await self._resolve_user_name(pubkey)
            source = self.build_source(
                chat_id=channel_id,
                chat_name=self._channel_names.get(channel_id, channel_id),
                chat_type="group",
                user_id=pubkey,
                user_name=user_name,
                thread_id=self._thread_root_id(event),
                message_id=str(event.get("id") or ""),
            )
            session_entry = store.get_or_create_session(source)
            attributed = f"[{user_name or pubkey}|{pubkey}]\n{content}"
            timestamp = (
                datetime.fromtimestamp(created_at).isoformat()
                if created_at
                else datetime.now().isoformat()
            )
            store.append_to_transcript(
                session_entry.session_id,
                {
                    "role": "user",
                    "content": attributed,
                    "timestamp": timestamp,
                    "message_id": str(event.get("id") or ""),
                    "observed": True,
                },
            )
            logger.info(
                "Buzz: observed unaddressed thread context in %s from %s…",
                channel_id,
                pubkey[:8],
            )
        except Exception as exc:
            logger.warning("Buzz: failed to observe unaddressed message: %s", exc)

    # ── DM classification (issue #68871) ──────────────────────────────────
    #
    # ``buzz dms list`` returns [] on some hosted relays even when DM
    # conversations exist, so DMs leak in via ``channels list`` and get
    # watched as chat_type="group" — which wrongly puts them behind the
    # channel mention gate.  Classification therefore keys off the Nostr
    # tags of the messages themselves.  Observed on a live hosted relay:
    #
    #   * every message another user sends IN A DM carries a structural
    #     ["p", <our pubkey>] tag, even when the text never mentions us
    #     (recipient addressing);
    #   * in a real channel, a ["p", <our pubkey>] tag appears only when the
    #     text visibly @mentions us (typed mention, with or without a reply
    #     ["e", ...] tag) — never on plain broadcasts.
    #
    # So "p-tagged to self WITHOUT a visible mention in the content" is the
    # DM discriminator: in a channel that combination does not occur, and a
    # channel reply/mention that p-tags us is excluded because the mention
    # is right there in the text.  As a second, independent guard, a
    # conversation whose ``channels list`` metadata looks like a real
    # community channel (real name / non-empty description) is never
    # reclassified at all, whereas relay-materialized DMs are always named
    # "DM" with an empty description.  Nothing is lost while unlatched: a
    # DM message that DOES mention us dispatches through the mention gate
    # anyway, so the latch flips exactly on the first message that needs it.

    def _may_reclassify_as_dm(self, channel_id: str) -> bool:
        """True when the conversation's metadata does not rule out a DM.

        Known real community channels (real name or non-empty description in
        ``channels list``) must never turn into DMs just because a message
        p-tags us.  A conversation with no metadata at all is trusted only
        when the user did not explicitly configure it as a watched channel.
        """
        meta = self._channel_meta.get(channel_id)
        if meta is None:
            return channel_id not in self.channels
        name = str(meta.get("name") or "").strip()
        description = str(meta.get("description") or "").strip()
        return name == "DM" and not description

    def _is_direct_message_event(self, channel_id: str, event: dict) -> bool:
        """True when ``event`` is shaped like a direct message to us: a chat
        message from another user, p-tagged to our pubkey, whose content does
        NOT visibly mention us — i.e. the p-tag is structural DM addressing,
        not the artifact of a typed @mention (see block comment above)."""
        if not self._self_pubkey or not self._may_reclassify_as_dm(channel_id):
            return False
        if int(event.get("kind") or 0) not in _STREAM_MESSAGE_KINDS:
            return False
        pubkey = str(event.get("pubkey") or "").lower()
        if not pubkey or pubkey == self._self_pubkey:
            return False
        tags = event.get("tags")
        if not isinstance(tags, list):
            return False
        p_tagged_to_self = any(
            isinstance(tag, (list, tuple))
            and len(tag) > 1
            and tag[0] == "p"
            and str(tag[1]).lower() == self._self_pubkey
            for tag in tags
        )
        if not p_tagged_to_self:
            return False
        content = event.get("content")
        return isinstance(content, str) and not self._is_mentioned(content)

    def _maybe_latch_dm(self, channel_id: str, state: dict, event: dict) -> None:
        """Latch a group conversation to chat_type="dm" once any direct
        message is seen; the classification then sticks so subsequent
        un-mentioned messages in the conversation dispatch too."""
        if state["chat_type"] == "dm" or not self._is_direct_message_event(
            channel_id, event
        ):
            return
        state["chat_type"] = "dm"
        self._channel_names.setdefault(channel_id, "DM")
        logger.info(
            "Buzz: conversation %s reclassified as DM (message p-tagged to self)",
            channel_id,
        )

    def _is_mentioned(self, content: str) -> bool:
        """True when the message addresses this agent (npub, hex, or name)."""
        lowered = content.lower()
        if self._self_pubkey and self._self_pubkey in lowered:
            return True
        if self._self_npub and self._self_npub in lowered:
            return True
        if self._display_name:
            pattern = rf"(?<!\w)@{re.escape(self._display_name.lower())}(?!\w)"
            if re.search(pattern, lowered):
                return True
        return False

    def _has_foreign_only_p_targets(self, event: dict) -> bool:
        """Whether an event explicitly targets identities other than this agent.

        Buzz clients encode resolved ``@Agent`` mentions as ``p`` tags. The
        primary agent's home channel remains mention-free for ordinary text,
        but a resolved specialist-only mention must not be captured by that
        convenience rule. Including this agent in the target set still counts
        as addressed, so ``@Nabu @Cosmo`` deliberately reaches both.
        """
        tags = event.get("tags")
        if not isinstance(tags, list):
            return False
        targets = {
            str(tag[1]).strip().lower()
            for tag in tags
            if isinstance(tag, (list, tuple))
            and len(tag) > 1
            and tag[0] == "p"
            and str(tag[1]).strip()
        }
        return bool(targets) and self._self_pubkey not in targets

    def _has_self_p_target(self, event: dict) -> bool:
        """Whether a signed event semantically addresses this agent.

        Orchestrated work deliberately keeps visible ``@Name`` text out of the
        root so the coordinator cannot accidentally trigger specialists in its
        own status message. The signed Nostr ``p`` tag is therefore the
        authoritative assignment signal and must pass a mention-required gate
        on its own.
        """
        if not self._self_pubkey:
            return False
        tags = event.get("tags")
        if not isinstance(tags, list):
            return False
        return any(
            isinstance(tag, (list, tuple))
            and len(tag) > 1
            and tag[0] == "p"
            and str(tag[1]).strip().lower() == self._self_pubkey
            for tag in tags
        )

    def _strip_mention(self, content: str) -> str:
        """Remove a leading @mention of this agent so the remaining text can be
        recognized as a slash command or clean prompt.

        Mirrors the Discord adapter, which strips its own ``<@id>`` mention
        before dispatch. Without this a channel message like ``@Chip /whoami``
        arrives with a leading ``@Chip``; the gateway's ``is_command()`` checks
        ``text.lstrip().startswith("/")`` and never fires the command. Only a
        LEADING mention is stripped (case-insensitive); mentions mid-sentence
        are left intact so normal prose is unaffected.
        """
        text = content.strip()
        candidates = []
        if self._display_name:
            candidates.append(re.escape(self._display_name))
        if self._self_npub:
            candidates.append(re.escape(self._self_npub))
        if self._self_pubkey:
            candidates.append(re.escape(self._self_pubkey))
        if not candidates:
            return text
        # Optional leading '@', one of the identity forms, optional trailing
        # ':' or ',' and surrounding whitespace.
        pattern = rf"^@?(?:{'|'.join(candidates)})[\s:,]*"
        stripped = re.sub(pattern, "", text, count=1, flags=re.IGNORECASE)
        return stripped.strip()

    async def _resolve_user_name(self, pubkey: str) -> str:
        """Resolve a pubkey to a display name (cached; falls back to npub prefix).

        Failures are cached too (negative caching): without it, every message
        from a profile-less pubkey re-runs ``users get`` each poll sweep,
        which amplifies badly when several adapter instances poll in one
        process.
        """
        cached = self._user_names.get(pubkey)
        if cached is not None:
            return cached
        name = ""
        code, out, _err = await self._run_cli(["users", "get", "--pubkey", pubkey])
        if code == 0:
            profiles = _parse_json_list(out)
            if profiles:
                name = str(profiles[0].get("display_name") or "").strip()
        if not name:
            name = (hex_to_npub(pubkey) or pubkey)[:16]
        self._user_names[pubkey] = name
        return name

    @staticmethod
    def _trim_seen(state: dict) -> None:
        seen = state["seen"]
        while len(seen) > _SEEN_CAP:
            seen.popitem(last=False)

    def _mark_seen(self, channel_id: str, event_id: str) -> None:
        state = self._channel_state.get(channel_id)
        if state is not None:
            state["seen"][event_id] = None
            self._trim_seen(state)

    async def _dispatch_message(
        self,
        text: str,
        chat_id: str,
        chat_type: str,
        user_id: str,
        user_name: str,
        message_id: str,
        created_at: int,
        thread_id: Optional[str] = None,
        raw_event: Optional[dict] = None,
    ) -> None:
        """Build a MessageEvent and hand it to the base class handler."""
        if not self._message_handler:
            return

        source = self.build_source(
            chat_id=chat_id,
            chat_name=self._channel_names.get(chat_id, chat_id),
            chat_type=chat_type,
            user_id=user_id,
            user_name=user_name,
            thread_id=thread_id,
            message_id=message_id,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message=raw_event,
            message_id=message_id,
            timestamp=datetime.fromtimestamp(created_at)
            if created_at
            else datetime.now(),
            channel_prompt=(
                "You are handling an addressed Buzz message with observed Buzz group context. "
                "Earlier Buzz messages may describe direct work "
                "with specialist agents, but were not requests to you. Treat only the current "
                "new message as addressed to you and use observed context when relevant."
                if chat_type == "group" and self.observe_unaddressed_messages
                else None
            ),
            auto_skill=resolve_channel_skills(self._extra, chat_id),
        )

        # Acknowledge receipt before processing. Every addressed profile owns a
        # separate adapter/key, so multi-agent mentions produce one auditable
        # reaction per agent without making the user wait for a long turn.
        try:
            await self.send_reaction(chat_id, message_id, "👀")
        except Exception:
            logger.debug(
                "Buzz: reaction failed for message %s", message_id[:12], exc_info=True
            )

        await self.handle_message(event)


# ---------------------------------------------------------------------------
# Plugin registration
# ---------------------------------------------------------------------------


def check_requirements() -> bool:
    """Check if Buzz is configured: a relay URL plus a resolvable key."""
    if not os.getenv("BUZZ_RELAY_URL", "").strip():
        return False
    return bool(_resolve_private_key())


def validate_config(config) -> bool:
    """Validate that the platform config has enough info to connect."""
    extra = getattr(config, "extra", {}) or {}
    relay = os.getenv("BUZZ_RELAY_URL") or extra.get("relay_url", "")
    return bool(relay and _resolve_private_key(extra))


def is_connected(config) -> bool:
    """Check whether Buzz is configured (env or config.yaml)."""
    return validate_config(config)


def _apply_yaml_config(yaml_cfg: dict, buzz_cfg: dict) -> Optional[dict]:
    """Translate ``config.yaml`` ``buzz.extra`` keys into ``BUZZ_*`` env vars.

    Implements the ``apply_yaml_config_fn`` contract.  ``check_requirements``
    and the adapter's connect path read configuration from the environment, so
    a config.yaml-only setup (no ``BUZZ_*`` env vars beyond the secret) would
    otherwise fail the ``check_fn`` gate and be silently skipped at gateway
    startup.  This hook bridges the ``extra`` block into env, mirroring the
    Slack/Telegram pattern.  Env vars win over YAML — every assignment is
    guarded by ``not os.getenv(...)`` so explicit env overrides survive a
    config.yaml update.  ``BUZZ_PRIVATE_KEY`` is a secret and stays in ``.env``;
    it is never sourced from config.yaml here.
    """
    extra = buzz_cfg.get("extra", buzz_cfg) or {}
    if not isinstance(extra, dict):
        return None
    _str_keys = {
        "relay_url": "BUZZ_RELAY_URL",
        "cli_path": "BUZZ_CLI_PATH",
        "home_channel": "BUZZ_HOME_CHANNEL",
        "transport": "BUZZ_TRANSPORT",
        "terminal_channel": "BUZZ_TERMINAL_CHANNEL",
        "terminal_cwd": "BUZZ_TERMINAL_CWD",
    }
    for src, env in _str_keys.items():
        val = extra.get(src)
        if val and not os.getenv(env):
            os.environ[env] = str(val)
    interval = extra.get("poll_interval")
    if interval is not None and not os.getenv("BUZZ_POLL_INTERVAL"):
        os.environ["BUZZ_POLL_INTERVAL"] = str(interval)
    channels = extra.get("channels")
    if channels is not None and not os.getenv("BUZZ_CHANNELS"):
        if isinstance(channels, (list, tuple)):
            channels = ",".join(str(c) for c in channels)
        os.environ["BUZZ_CHANNELS"] = str(channels)
    allowed = extra.get("allowed_users")
    if allowed is not None and not os.getenv("BUZZ_ALLOWED_USERS"):
        if isinstance(allowed, (list, tuple)):
            allowed = ",".join(str(a) for a in allowed)
        os.environ["BUZZ_ALLOWED_USERS"] = str(allowed)
    terminal_allowed = extra.get("terminal_allowed_users")
    if terminal_allowed is not None and not os.getenv("BUZZ_TERMINAL_ALLOWED_USERS"):
        if isinstance(terminal_allowed, (list, tuple)):
            terminal_allowed = ",".join(str(a) for a in terminal_allowed)
        os.environ["BUZZ_TERMINAL_ALLOWED_USERS"] = str(terminal_allowed)
    if "allow_all_users" in extra and not os.getenv("BUZZ_ALLOW_ALL_USERS"):
        os.environ["BUZZ_ALLOW_ALL_USERS"] = str(extra["allow_all_users"]).lower()
    if "require_mention" in extra and not os.getenv("BUZZ_REQUIRE_MENTION"):
        os.environ["BUZZ_REQUIRE_MENTION"] = str(extra["require_mention"]).lower()
    if "terminal_enabled" in extra and not os.getenv("BUZZ_TERMINAL_ENABLED"):
        os.environ["BUZZ_TERMINAL_ENABLED"] = str(extra["terminal_enabled"]).lower()
    return None


def _env_enablement() -> Optional[dict]:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    Called BEFORE adapter construction so env-only setups show up in
    ``hermes gateway status`` and ``get_connected_platforms()``.  Returns
    ``None`` when Buzz isn't minimally configured.

    The special ``home_channel`` key is handled by the core hook — it becomes
    a proper ``HomeChannel`` on the ``PlatformConfig``.
    """
    relay = os.getenv("BUZZ_RELAY_URL", "").strip()
    if not relay or not _resolve_private_key():
        return None
    seed: dict = {"relay_url": relay}
    channels = os.getenv("BUZZ_CHANNELS", "").strip()
    if channels:
        seed["channels"] = [c.strip() for c in channels.split(",") if c.strip()]
    interval = os.getenv("BUZZ_POLL_INTERVAL", "").strip()
    if interval:
        try:
            seed["poll_interval"] = float(interval)
        except ValueError:
            pass
    cli_path = os.getenv("BUZZ_CLI_PATH", "").strip()
    if cli_path:
        seed["cli_path"] = cli_path
    terminal_enabled = os.getenv("BUZZ_TERMINAL_ENABLED", "").strip()
    if terminal_enabled:
        seed["terminal_enabled"] = terminal_enabled.lower() in (
            "true",
            "1",
            "yes",
            "on",
        )
    terminal_channel = os.getenv("BUZZ_TERMINAL_CHANNEL", "").strip()
    if terminal_channel:
        seed["terminal_channel"] = terminal_channel
    terminal_cwd = os.getenv("BUZZ_TERMINAL_CWD", "").strip()
    if terminal_cwd:
        seed["terminal_cwd"] = terminal_cwd
    terminal_allowed = os.getenv("BUZZ_TERMINAL_ALLOWED_USERS", "").strip()
    if terminal_allowed:
        seed["terminal_allowed_users"] = [
            entry.strip() for entry in terminal_allowed.split(",") if entry.strip()
        ]
    # Home channel for deliver=buzz cron jobs; defaults to the first watched
    # channel so env-only setups get a sensible target without extra config.
    home = (
        os.getenv("BUZZ_HOME_CHANNEL", "").strip() or (seed.get("channels") or [""])[0]
    )
    if home:
        seed["home_channel"] = {
            "chat_id": home,
            "name": os.getenv("BUZZ_HOME_CHANNEL_NAME", home),
        }
    return seed


async def _standalone_send(
    pconfig,
    chat_id: str,
    message: str,
    *,
    thread_id: Optional[str] = None,
    media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """One-shot send without a live adapter (out-of-process cron delivery).

    Used by ``tools/send_message_tool`` when ``hermes cron`` runs separately
    from the gateway process.  Without this hook, ``deliver=buzz`` cron jobs
    fail with ``No live adapter for platform 'buzz'``.
    """
    extra = getattr(pconfig, "extra", {}) or {}
    relay = (os.getenv("BUZZ_RELAY_URL") or extra.get("relay_url", "")).strip()
    private_key = _resolve_private_key(extra)
    cli_path = _resolve_cli_path(
        os.getenv("BUZZ_CLI_PATH", "").strip() or str(extra.get("cli_path", "") or "")
    )
    if not relay or not private_key:
        return {
            "error": "Buzz standalone send: BUZZ_RELAY_URL and BUZZ_PRIVATE_KEY must be configured"
        }
    if not cli_path:
        return {"error": "Buzz standalone send: buzz CLI binary not found"}
    target = (chat_id or "").strip() or (
        os.getenv("BUZZ_HOME_CHANNEL") or str(extra.get("home_channel", "") or "")
    ).strip()
    if not target:
        return {
            "error": "Buzz standalone send: no target channel (set BUZZ_HOME_CHANNEL)"
        }

    args = ["messages", "send", "--channel", target, "--content", "-"]
    if thread_id:
        args += ["--reply-to", str(thread_id)]
    for path in media_files or []:
        args += ["--file", str(path)]
    try:
        code, out, err = await _exec_buzz(
            cli_path, args, relay_url=relay, private_key=private_key, input_text=message
        )
    except asyncio.CancelledError:
        raise
    except OSError as e:
        return {"error": f"Buzz standalone send failed to launch CLI: {e}"}
    if code != 0:
        return {
            "error": f"Buzz standalone send failed: {_cli_error_message(err, code)}"
        }
    try:
        data = json.loads(out or "{}")
    except ValueError:
        data = {}
    return {"success": True, "message_id": str(data.get("event_id") or "")}


def interactive_setup() -> None:
    """Interactive ``hermes gateway setup`` flow for the Buzz platform.

    Lazy-imports ``hermes_cli.setup`` helpers so the plugin stays importable
    in non-CLI contexts (gateway runtime, tests).
    """
    from hermes_cli.setup import (
        prompt,
        prompt_yes_no,
        save_env_value,
        get_env_value,
        print_header,
        print_info,
        print_warning,
        print_success,
    )

    print_header("Buzz")
    existing_relay = get_env_value("BUZZ_RELAY_URL")
    if existing_relay:
        print_info(f"Buzz: already configured (relay: {existing_relay})")
        if not prompt_yes_no("Reconfigure Buzz?", False):
            return

    print_info(
        "Connect Hermes to a Buzz community (Block's Nostr-based human+agent platform)."
    )
    print_info(
        "   Requires the buzz CLI binary and a Nostr key that is a community member."
    )
    print()

    relay = prompt(
        "Relay URL (e.g. https://mycommunity.communities.buzz.xyz)",
        default=existing_relay or "",
    )
    if not relay:
        print_warning("Relay URL is required — skipping Buzz setup")
        return
    save_env_value("BUZZ_RELAY_URL", relay.strip())

    key = prompt(
        "Nostr private key (nsec or hex; leave blank to keep current)", password=True
    )
    if key:
        save_env_value("BUZZ_PRIVATE_KEY", key.strip())
    elif not _resolve_private_key():
        print_warning(
            "No private key configured — set BUZZ_PRIVATE_KEY before starting the gateway"
        )

    channels = prompt(
        "Channel UUIDs to watch (comma-separated, empty = all joined channels)",
        default=get_env_value("BUZZ_CHANNELS") or "",
    )
    if channels:
        save_env_value("BUZZ_CHANNELS", channels.replace(" ", ""))

    home = prompt(
        "Home channel UUID for cron/notification delivery (optional)",
        default=get_env_value("BUZZ_HOME_CHANNEL") or "",
    )
    if home:
        save_env_value("BUZZ_HOME_CHANNEL", home.strip())

    print()
    print_info("🔒 Access control: restrict who can talk to the agent")
    allow_all = prompt_yes_no(
        "Allow all community members to talk to the agent?", False
    )
    if allow_all:
        save_env_value("BUZZ_ALLOW_ALL_USERS", "true")
        save_env_value("BUZZ_ALLOWED_USERS", "")
        print_warning("⚠️  Open access — anyone in the community can command the agent.")
    else:
        save_env_value("BUZZ_ALLOW_ALL_USERS", "false")
        allowed = prompt(
            "Allowed users (comma-separated npubs or hex pubkeys, empty to deny everyone)",
            default=get_env_value("BUZZ_ALLOWED_USERS") or "",
        )
        save_env_value(
            "BUZZ_ALLOWED_USERS", allowed.replace(" ", "") if allowed else ""
        )

    print()
    print_success("Buzz configuration saved to ~/.hermes/.env")
    print_info("Restart the gateway for changes to take effect: hermes gateway restart")


def register(ctx):
    """Plugin entry point: called by the Hermes plugin system."""
    ctx.register_platform(
        name="buzz",
        label="Buzz",
        adapter_factory=lambda cfg: BuzzAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["BUZZ_RELAY_URL", "BUZZ_PRIVATE_KEY"],
        install_hint="Requires the buzz CLI binary (https://github.com/block/buzz) on PATH or at BUZZ_CLI_PATH",
        setup_fn=interactive_setup,
        # Env-driven auto-configuration: seeds PlatformConfig.extra with
        # relay/channels/poll interval + home_channel so env-only setups show
        # up in gateway status without instantiating the adapter.
        env_enablement_fn=_env_enablement,
        # Bridge config.yaml buzz.extra -> BUZZ_* env vars so check_fn and the
        # env-driven connect path work for config.yaml-only setups (secret stays
        # in .env). Without this the check_fn gate skips Buzz at startup.
        apply_yaml_config_fn=_apply_yaml_config,
        # Cron home-channel delivery support (deliver=buzz).
        cron_deliver_env_var="BUZZ_HOME_CHANNEL",
        # Out-of-process cron delivery.  Without this hook, deliver=buzz
        # cron jobs fail with "No live adapter" when cron runs separately
        # from the gateway.
        standalone_sender_fn=_standalone_send,
        # Auth env vars for _is_user_authorized() integration
        allowed_users_env="BUZZ_ALLOWED_USERS",
        allow_all_env="BUZZ_ALLOW_ALL_USERS",
        # Display
        emoji="🐝",
        # Buzz identities are pubkeys, not phone numbers
        pii_safe=False,
        allow_update_command=True,
        # LLM guidance
        platform_hint=(
            "You are collaborating in a Buzz workspace (Block's Nostr-based "
            "human+agent platform). Markdown IS supported. Users address you "
            "by @-mentioning your name or npub in channels; direct messages "
            "reach you without a mention. In an owner-configured coordinator "
            "channel, use buzz_orchestrate to create and assign a real Buzz "
            "work thread instead of simulating delegation in prose. Keep "
            "responses conversational."
        ),
    )
