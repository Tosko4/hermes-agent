from __future__ import annotations

import json

import pytest

from gateway.session_context import clear_session_vars, set_session_vars
from plugins.platforms.buzz import adapter as buzz_adapter
from plugins.platforms.buzz import tools as buzz_tools


OWNER = "b" * 64
AGENT = "c" * 64
SOURCE_EVENT = "d" * 64
RESULT_EVENT = "e" * 64
HOME = "812dd8b8-ffd3-5619-8414-18df079fcce6"
TARGET = "9f729e79-9115-42e7-80db-4ee1664f3bfa"


class FakeState:
    def __init__(self):
        self.values = {}

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


def _config():
    return {
        "gateway": {
            "platforms": {
                "buzz": {
                    "extra": {
                        "relay_url": "https://buzz.example",
                        "cli_path": "/opt/buzz",
                        "orchestration": {
                            "enabled": True,
                            "home_channel": HOME,
                            "allowed_users": [OWNER],
                            "agents": {"Cosmo": AGENT},
                            "routes": {
                                "research": {
                                    "channel_id": TARGET,
                                    "label": "research",
                                    "kind": "forum",
                                    "agents": ["Cosmo"],
                                }
                            },
                        },
                    }
                }
            }
        }
    }


@pytest.fixture
def owner_context():
    tokens = set_session_vars(
        platform="buzz",
        chat_id=HOME,
        user_id=OWNER,
        message_id=SOURCE_EVENT,
        session_key="buzz:test",
    )
    try:
        yield
    finally:
        clear_session_vars(tokens)


@pytest.fixture
def configured(monkeypatch, owner_context):
    monkeypatch.setattr(buzz_tools, "_runtime_config", _config)
    monkeypatch.setattr(
        buzz_adapter, "_resolve_cli_path", lambda _value="": "/opt/buzz"
    )
    monkeypatch.setattr(
        buzz_adapter, "_resolve_private_key", lambda _extra=None: "private-test-key"
    )


@pytest.mark.asyncio
async def test_creates_titled_forum_root_with_exact_mentions_and_origin(
    configured, monkeypatch
):
    calls = []

    async def fake_exec(path, args, **kwargs):
        calls.append((path, args, kwargs))
        return 0, json.dumps({"event_id": RESULT_EVENT, "accepted": True}), ""

    monkeypatch.setattr(buzz_adapter, "_exec_buzz", fake_exec)
    state = FakeState()
    result = await buzz_tools.buzz_orchestrate(
        {
            "route": "research",
            "title": "Onderzoek push delivery",
            "task": "Verifieer de volledige Firebase-keten met primaire bronnen.",
            "agents": ["Cosmo"],
        },
        state=state,
    )

    assert f"buzz://message?channel={TARGET}&id={RESULT_EVENT}" in result
    assert "@Cosmo" in result
    assert len(calls) == 1
    path, args, kwargs = calls[0]
    assert path == "/opt/buzz"
    assert args == [
        "messages",
        "send",
        "--channel",
        TARGET,
        "--content",
        "-",
        "--kind",
        "45001",
        "--title",
        "Onderzoek push delivery",
        "--mention",
        AGENT,
    ]
    assert kwargs["private_key"] == "private-test-key"
    assert kwargs["relay_url"] == "https://buzz.example"
    assert kwargs["input_text"].startswith(
        "Verifieer de volledige Firebase-keten met primaire bronnen.\n\n"
    )
    assert (
        f"Bron: buzz://message?channel={HOME}&id={SOURCE_EVENT}" in kwargs["input_text"]
    )
    assert "Opdracht voor: @Cosmo" in kwargs["input_text"]
    assert next(iter(state.values.values()))["status"] == "complete"


@pytest.mark.asyncio
async def test_exact_retry_returns_cached_link_without_second_publish(
    configured, monkeypatch
):
    calls = 0

    async def fake_exec(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return 0, json.dumps({"event_id": RESULT_EVENT, "accepted": True}), ""

    monkeypatch.setattr(buzz_adapter, "_exec_buzz", fake_exec)
    state = FakeState()
    request = {
        "route": "research",
        "title": "Een concrete titel",
        "task": "Doe een afgebakend onderzoek.",
        "agents": ["Cosmo"],
    }
    first = await buzz_tools.buzz_orchestrate(request, state=state)
    second = await buzz_tools.buzz_orchestrate(request, state=state)

    assert first == second
    assert calls == 1


@pytest.mark.asyncio
async def test_indeterminate_network_result_blocks_blind_retry(configured, monkeypatch):
    calls = 0

    async def fake_exec(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        return 2, "", json.dumps({"error": "network", "message": "reply lost"})

    monkeypatch.setattr(buzz_adapter, "_exec_buzz", fake_exec)
    state = FakeState()
    request = {
        "route": "research",
        "title": "Netwerkresultaat controleren",
        "task": "Controleer dit eenmalig.",
        "agents": ["Cosmo"],
    }
    first = await buzz_tools.buzz_orchestrate(request, state=state)
    second = await buzz_tools.buzz_orchestrate(request, state=state)

    assert "reply lost" in first
    assert "reconcile" in second.lower()
    assert calls == 1
    assert next(iter(state.values.values()))["status"] == "indeterminate"


@pytest.mark.asyncio
async def test_rejects_unconfigured_route_and_specialist_before_publish(
    configured, monkeypatch
):
    async def should_not_run(*_args, **_kwargs):
        raise AssertionError("CLI must not run")

    monkeypatch.setattr(buzz_adapter, "_exec_buzz", should_not_run)
    base = {"title": "Geldige titel", "task": "Geldige opdracht."}
    unknown_route = await buzz_tools.buzz_orchestrate(
        {**base, "route": "secrets", "agents": ["Cosmo"]}, state=FakeState()
    )
    unknown_agent = await buzz_tools.buzz_orchestrate(
        {**base, "route": "research", "agents": ["Mallory"]}, state=FakeState()
    )

    assert "Unknown route" in unknown_route
    assert "not allowed" in unknown_agent


@pytest.mark.asyncio
async def test_rejects_non_owner_and_non_home_context(monkeypatch):
    monkeypatch.setattr(buzz_tools, "_runtime_config", _config)
    tokens = set_session_vars(
        platform="buzz",
        chat_id=TARGET,
        user_id="a" * 64,
        message_id=SOURCE_EVENT,
    )
    try:
        # Registration is process-scoped and therefore configuration-only;
        # the handler performs context-sensitive authorization on every call.
        assert buzz_tools._orchestration_configured() is True
        result = await buzz_tools.buzz_orchestrate(
            {
                "route": "research",
                "title": "Geldige titel",
                "task": "Geldige opdracht.",
                "agents": ["Cosmo"],
            },
            state=FakeState(),
        )
    finally:
        clear_session_vars(tokens)

    assert "restricted" in result


@pytest.mark.asyncio
async def test_requires_origin_event_and_bounded_fields(configured, monkeypatch):
    too_long = await buzz_tools.buzz_orchestrate(
        {
            "route": "research",
            "title": "x" * 81,
            "task": "body",
            "agents": ["Cosmo"],
        },
        state=FakeState(),
    )
    assert "80" in too_long

    clear_session_vars([])
    tokens = set_session_vars(
        platform="buzz", chat_id=HOME, user_id=OWNER, message_id=""
    )
    try:
        missing_origin = await buzz_tools.buzz_orchestrate(
            {
                "route": "research",
                "title": "Geldige titel",
                "task": "body",
                "agents": ["Cosmo"],
            },
            state=FakeState(),
        )
    finally:
        clear_session_vars(tokens)
    assert "originating Buzz event ID" in missing_origin


def test_registration_uses_process_stable_gate(monkeypatch):
    registrations = []

    class Context:
        state = FakeState()

        @staticmethod
        def register_tool(**kwargs):
            registrations.append(kwargs)

    monkeypatch.setattr(buzz_tools, "_runtime_config", _config)
    buzz_tools.register_tools(Context())

    assert len(registrations) == 1
    registration = registrations[0]
    assert registration["name"] == "buzz_orchestrate"
    assert registration["toolset"] == "buzz"
    assert registration["check_fn"] is buzz_tools._orchestration_configured
    assert registration["check_fn"]() is True


def test_static_gate_rejects_incomplete_configuration(monkeypatch):
    broken = _config()
    broken["gateway"]["platforms"]["buzz"]["extra"]["orchestration"][
        "allowed_users"
    ] = []
    monkeypatch.setattr(buzz_tools, "_runtime_config", lambda: broken)

    assert buzz_tools._orchestration_configured() is False
