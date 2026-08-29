"""Tests for the Buzz platform adapter plugin."""

import asyncio
import json

import pytest
from unittest.mock import AsyncMock, MagicMock

from tests.gateway._plugin_adapter_loader import load_plugin_adapter

# Load plugins/platforms/buzz/adapter.py under a unique module name
# (plugin_adapter_buzz) so it cannot collide with other plugin adapters
# loaded by sibling tests in the same xdist worker.
_buzz_mod = load_plugin_adapter("buzz")

BuzzAdapter = _buzz_mod.BuzzAdapter
hex_to_npub = _buzz_mod.hex_to_npub
npub_to_hex = _buzz_mod.npub_to_hex
_normalize_user_ref = _buzz_mod._normalize_user_ref
_cli_error_message = _buzz_mod._cli_error_message
_resolve_private_key = _buzz_mod._resolve_private_key
check_requirements = _buzz_mod.check_requirements
validate_config = _buzz_mod.validate_config
register = _buzz_mod.register
_env_enablement = _buzz_mod._env_enablement
_standalone_send = _buzz_mod._standalone_send

# Real key pair (Chip's public identity — public information, not a secret)
SELF_PUBKEY = "9fd5c7ba6d3ef224da78f541e0fcb9c50f72cc63edb19aae76ac6a0474dfa860"
SELF_NPUB = "npub1nl2u0wnd8mezfknc74q7pl9ec58h9nrrakce4tnk434qgaxl4psqe5twr6"
OTHER_PUBKEY = "a" * 64
CHANNEL = "ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd"
# Real DM conversation as materialized by a hosted relay: `dms list` returns
# [] for it (#68871) while `channels list` shows it as name "DM", empty
# description, indistinguishable from a channel except via message p-tags.
DM_CHANNEL = "6468cc16-a114-4f23-8b8c-02c1655cbf6b"

_ENV_VARS = (
    "BUZZ_RELAY_URL",
    "BUZZ_PRIVATE_KEY",
    "BUZZ_CHANNELS",
    "BUZZ_HOME_CHANNEL",
    "BUZZ_ALLOWED_USERS",
    "BUZZ_ALLOW_ALL_USERS",
    "BUZZ_REQUIRE_MENTION",
    "BUZZ_POLL_INTERVAL",
    "BUZZ_CLI_PATH",
    "BUZZ_CREDENTIALS_FILE",
    "BUZZ_TERMINAL_ENABLED",
    "BUZZ_TERMINAL_CHANNEL",
    "BUZZ_TERMINAL_ALLOWED_USERS",
    "BUZZ_TERMINAL_CWD",
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch, tmp_path):
    """Keep tests hermetic: no ambient Buzz env vars or real credentials."""
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(_buzz_mod, "_DEFAULT_CREDENTIALS_DIR", tmp_path / "no-creds")
    yield


def _event(event_id, pubkey=OTHER_PUBKEY, content="hello", created_at=1000, kind=9):
    return {
        "id": event_id,
        "pubkey": pubkey,
        "content": content,
        "created_at": created_at,
        "kind": kind,
        "tags": [["h", CHANNEL]],
    }


def _make_adapter(extra=None):
    from gateway.config import PlatformConfig

    cfg = PlatformConfig(
        enabled=True, extra={"relay_url": "https://test.relay", **(extra or {})}
    )
    adapter = BuzzAdapter(cfg)
    adapter._self_pubkey = SELF_PUBKEY
    adapter._self_npub = SELF_NPUB
    adapter._display_name = "Chip"
    adapter._private_key = "nsec1test"
    return adapter


class _ScriptedCli:
    """Fake ``_run_cli`` that routes on the buzz subcommand and records calls."""

    def __init__(self):
        self.responses = {}  # (group, cmd) -> list of (code, stdout, stderr)
        self.calls = []

    def script(self, group, cmd, payload, code=0, stderr=""):
        stdout = payload if isinstance(payload, str) else json.dumps(payload)
        self.responses.setdefault((group, cmd), []).append((code, stdout, stderr))

    async def __call__(self, args, *, input_text=None):
        self.calls.append((list(args), input_text))
        queue = self.responses.get((args[0], args[1]), [])
        if len(queue) > 1:
            return queue.pop(0)
        if queue:
            return queue[0]
        return 0, "[]", ""


# ── bech32 / identity helpers ─────────────────────────────────────────────


class TestBech32Helpers:
    def test_hex_to_npub_known_pair(self):
        assert hex_to_npub(SELF_PUBKEY) == SELF_NPUB

    def test_npub_to_hex_known_pair(self):
        assert npub_to_hex(SELF_NPUB) == SELF_PUBKEY


# ── Adapter init / config precedence ──────────────────────────────────────


class TestBuzzAdapterInit:
    def test_terminal_broker_loads_under_single_file_plugin_loader(self):
        broker_type = _buzz_mod._load_terminal_broker()
        assert broker_type.__name__ == "TerminalBroker"

    def test_init_from_config_extra(self):
        from gateway.config import PlatformConfig

        cfg = PlatformConfig(
            enabled=True,
            extra={
                "relay_url": "https://cfg.relay",
                "channels": ["ccc"],
                "poll_interval": 2,
                "home_channel": "ccc",
                "require_mention": True,
                "accept_bare_slash_commands": True,
                "observe_unaddressed_messages": True,
                "terminal_enabled": True,
                "terminal_channel": CHANNEL,
                "terminal_allowed_users": [OTHER_PUBKEY],
                "terminal_cwd": "/tmp",
            },
        )
        adapter = BuzzAdapter(cfg)
        assert adapter.relay_url == "https://cfg.relay"
        assert adapter.channels == ["ccc"]
        assert adapter.poll_interval == 2.0
        assert adapter.home_channel == "ccc"
        assert adapter.require_mention is True
        assert adapter.accept_bare_slash_commands is True
        assert adapter.observe_unaddressed_messages is True
        assert adapter.terminal_enabled is True
        assert adapter.terminal_channel == CHANNEL
        assert adapter._terminal_allowed_pubkeys == {OTHER_PUBKEY}
        assert adapter.terminal_cwd == "/tmp"

    def test_primary_agent_behavior_stays_profile_scoped(self, monkeypatch):
        monkeypatch.setenv("BUZZ_ACCEPT_BARE_SLASH_COMMANDS", "true")
        monkeypatch.setenv("BUZZ_OBSERVE_UNADDRESSED_MESSAGES", "true")
        from gateway.config import PlatformConfig

        specialist = BuzzAdapter(
            PlatformConfig(
                enabled=True,
                extra={
                    "accept_bare_slash_commands": False,
                    "observe_unaddressed_messages": False,
                },
            )
        )

        assert specialist.accept_bare_slash_commands is False
        assert specialist.observe_unaddressed_messages is False

    def test_env_overrides_config(self, monkeypatch):
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://env.relay")
        from gateway.config import PlatformConfig

        adapter = BuzzAdapter(
            PlatformConfig(enabled=True, extra={"relay_url": "https://cfg.relay"})
        )
        assert adapter.relay_url == "https://env.relay"


# ── CLI error contract ────────────────────────────────────────────────────


class TestCliErrorContract:
    def test_parses_json_error(self):
        msg = _cli_error_message(
            '{"error":"relay_error","message":"boom","retryable":false}', 2
        )
        assert "relay_error" in msg and "boom" in msg and "exit 2" in msg


# ── Seeding / high-water mark / de-dupe ───────────────────────────────────


class TestPollingDedupe:
    @pytest.fixture
    def adapter(self):
        a = _make_adapter()
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        return a

    @pytest.mark.asyncio
    async def test_seed_sets_high_water_mark_without_dispatch(self, adapter):
        cli = _ScriptedCli()
        cli.script(
            "messages",
            "get",
            [
                _event("e1", content="@Chip old history", created_at=100),
                _event("e2", content="@Chip newer history", created_at=200),
            ],
        )
        adapter._run_cli = cli
        await adapter._seed_channel(CHANNEL, chat_type="group")

        state = adapter._channel_state[CHANNEL]
        assert state["last_ts"] == 200
        assert set(state["seen"]) == {"e1", "e2"}
        # Seeding must never replay history into the agent
        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_new_event_dispatched_once(self, adapter):
        cli = _ScriptedCli()
        cli.script(
            "messages", "get", [_event("e1", content="@Chip hi", created_at=100)]
        )
        adapter._run_cli = cli
        await adapter._seed_channel(CHANNEL, chat_type="group")

        # Poll 1: seeded event + a genuinely new mention
        cli.responses.clear()
        cli.script(
            "messages",
            "get",
            [
                _event("e1", content="@Chip hi", created_at=100),
                _event("e2", content="hey @Chip, ping", created_at=150),
            ],
        )
        await adapter._poll_channel(CHANNEL)
        assert [d["message_id"] for d in adapter._dispatched] == ["e2"]
        assert adapter._dispatched[0]["text"] == "hey @Chip, ping"
        assert adapter._channel_state[CHANNEL]["last_ts"] == 150

        # Poll 2: identical response — the seen-id set must de-dupe
        await adapter._poll_channel(CHANNEL)
        assert len(adapter._dispatched) == 1


# ── Mention gating / DMs / authorization ──────────────────────────────────


class TestMentionGating:
    @pytest.fixture
    def adapter(self):
        a = _make_adapter()
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        a._channel_state[CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        return a

    async def _poll_with(self, adapter, *events):
        cli = _ScriptedCli()
        cli.script("messages", "get", list(events))
        adapter._run_cli = cli
        await adapter._poll_channel(CHANNEL)

    @pytest.mark.asyncio
    async def test_unaddressed_channel_message_ignored(self, adapter):
        await self._poll_with(
            adapter, _event("e1", content="just chatting", created_at=10)
        )
        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_signed_self_p_tag_dispatches_without_visible_at_name(self, adapter):
        event = _event(
            "e1",
            content="Primaire uitvoerder: Chip\nControleer de callbackketen.",
            created_at=10,
        )
        event["tags"].append(["p", SELF_PUBKEY])

        await self._poll_with(adapter, event)

        assert [item["message_id"] for item in adapter._dispatched] == ["e1"]

    @pytest.mark.asyncio
    async def test_channel_policy_can_enable_implicit_pickup(self, adapter):
        cli = _ScriptedCli()
        cli.script(
            "channels",
            "get",
            {
                "channel_id": CHANNEL,
                "name": "research",
                "agent_mentions": "optional",
            },
        )
        cli.script(
            "messages",
            "get",
            [_event("e1", content="investigate this", created_at=10)],
        )
        adapter._run_cli = cli

        await adapter._poll_channel(CHANNEL)

        assert [item["message_id"] for item in adapter._dispatched] == ["e1"]

    @pytest.mark.asyncio
    async def test_explicit_channel_policy_can_require_mentions_in_home(self, adapter):
        adapter.home_channel = CHANNEL
        cli = _ScriptedCli()
        cli.script(
            "channels",
            "get",
            {
                "channel_id": CHANNEL,
                "name": "nabu",
                "agent_mentions": "required",
            },
        )
        cli.script(
            "messages",
            "get",
            [_event("e1", content="do not pick this up", created_at=10)],
        )
        adapter._run_cli = cli

        await adapter._poll_channel(CHANNEL)

        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_thread_policy_overrides_required_channel(self, adapter):
        root = _event("a" * 64, content="start", created_at=10)
        root["tags"].extend(
            [["subject", "Agent work"], ["agent_mentions", "optional"]]
        )
        cli = _ScriptedCli()
        cli.script(
            "messages",
            "get",
            [root],
        )
        adapter._run_cli = cli

        await adapter._poll_channel(CHANNEL)

        assert [item["message_id"] for item in adapter._dispatched] == ["a" * 64]

    def test_malformed_or_duplicate_thread_policy_fails_closed(self, adapter):
        malformed = _event("a" * 64)
        malformed["tags"].append(["agent_mentions", "sometimes"])
        assert adapter._agent_mention_policy(malformed) is True

        duplicate = _event("b" * 64)
        duplicate["tags"].extend(
            [
                ["agent_mentions", "optional"],
                ["agent_mentions", "optional"],
            ]
        )
        assert adapter._agent_mention_policy(duplicate) is True

    def test_agent_specific_thread_policy_overrides_default(self, adapter):
        event = _event("a" * 64)
        event["tags"].extend(
            [
                ["agent_mentions", "required"],
                ["agent_mentions", SELF_PUBKEY, "optional"],
                ["agent_mentions", "b" * 64, "required"],
            ]
        )
        assert adapter._agent_mention_policy(event, SELF_PUBKEY) is False
        assert adapter._agent_mention_policy(event, "b" * 64) is True

    @pytest.mark.asyncio
    async def test_agent_specific_channel_policy_overrides_default(self, adapter):
        adapter._self_pubkey = SELF_PUBKEY
        cli = _ScriptedCli()
        cli.script(
            "channels",
            "get",
            {
                "channel_id": CHANNEL,
                "name": "research",
                "agent_mentions": "required",
                "agent_mention_overrides": {SELF_PUBKEY: "optional"},
            },
        )
        adapter._run_cli = cli

        assert await adapter._effective_mention_required(CHANNEL, _event("e1")) is False

    @pytest.mark.asyncio
    async def test_latest_thread_edit_updates_policy_without_loading_history(
        self, adapter
    ):
        root_id = "a" * 64
        reply = _event("e1", content="continue", created_at=30)
        reply["tags"].append(["e", root_id, "", "root"])
        root = _event(root_id, content="start", created_at=10)
        root["tags"].append(["subject", "Agent work"])
        edit = _event("b" * 64, content="start", created_at=20, kind=40003)
        edit["tags"].extend(
            [
                ["e", root_id],
                ["subject", "Agent work"],
                ["agent_mentions", "optional"],
            ]
        )
        cli = _ScriptedCli()
        cli.script("messages", "thread", [root, edit])
        adapter._run_cli = cli

        assert await adapter._effective_mention_required(CHANNEL, reply) is False
        assert "--root-only" in cli.calls[0][0]

    @pytest.mark.asyncio
    async def test_optional_channel_respects_foreign_only_target(self, adapter):
        event = _event("e1", content="@Cosmo own this", created_at=10)
        event["tags"].append(["p", "b" * 64])
        cli = _ScriptedCli()
        cli.script(
            "channels",
            "get",
            {
                "channel_id": CHANNEL,
                "name": "research",
                "agent_mentions": "optional",
            },
        )
        cli.script("messages", "get", [event])
        adapter._run_cli = cli

        await adapter._poll_channel(CHANNEL)

        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_unaddressed_specialist_message_is_observed_without_dispatch(
        self, adapter
    ):
        adapter.observe_unaddressed_messages = True
        adapter._resolve_user_name = AsyncMock(return_value="Helper Bot")
        store = MagicMock()
        store.get_or_create_session.return_value.session_id = "session-root"
        adapter.set_session_store(store)
        event = _event("nested", content="@OtherAgent /status", created_at=10)
        event["tags"].extend([
            ["e", "outer-root", "", "root"],
            ["e", "parent-reply", "", "reply"],
        ])

        await self._poll_with(adapter, event)

        assert adapter._dispatched == []
        source = store.get_or_create_session.call_args.args[0]
        assert source.thread_id == "outer-root"
        store.append_to_transcript.assert_called_once()
        transcript = store.append_to_transcript.call_args.args[1]
        assert transcript["observed"] is True
        assert transcript["message_id"] == "nested"
        assert transcript["content"].endswith("@OtherAgent /status")

    @pytest.mark.asyncio
    async def test_name_mention_dispatched(self, adapter):
        await self._poll_with(
            adapter, _event("e1", content="hey @Chip can you help?", created_at=10)
        )
        assert len(adapter._dispatched) == 1

    @pytest.mark.asyncio
    async def test_addressed_forum_post_dispatches_with_post_as_thread_root(
        self, adapter
    ):
        post_id = "f" * 64
        await self._poll_with(
            adapter,
            _event(
                post_id,
                content="@Chip investigate this topic",
                created_at=10,
                kind=45001,
            ),
        )

        assert adapter._channel_state[CHANNEL]["forum"] is True
        assert [d["message_id"] for d in adapter._dispatched] == [post_id]
        assert adapter._dispatched[0]["thread_id"] == post_id

    @pytest.mark.asyncio
    async def test_addressed_forum_comment_uses_outer_post_root(self, adapter):
        post_id = "a" * 64
        comment = _event(
            "c" * 64,
            content="@Chip follow up",
            created_at=11,
            kind=45003,
        )
        comment["tags"].extend([
            ["e", post_id, "", "root"],
            ["e", "b" * 64, "", "reply"],
        ])

        await self._poll_with(adapter, comment)

        assert [d["thread_id"] for d in adapter._dispatched] == [post_id]

    @pytest.mark.asyncio
    async def test_structured_stream_message_is_not_silently_dropped(self, adapter):
        await self._poll_with(
            adapter,
            _event(
                "2" * 64,
                content="@Chip structured stream message",
                created_at=12,
                kind=40002,
            ),
        )

        assert [d["message_id"] for d in adapter._dispatched] == ["2" * 64]

    @pytest.mark.asyncio
    async def test_plain_name_is_not_a_mention(self, adapter):
        await self._poll_with(
            adapter, _event("e1", content="Chip can help with this", created_at=10)
        )
        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_home_channel_does_not_require_a_mention(self, adapter):
        adapter.home_channel = CHANNEL
        adapter._channel_policy_checked_at[CHANNEL] = float("inf")
        await self._poll_with(
            adapter, _event("e1", content="pick this up", created_at=10)
        )
        assert [d["message_id"] for d in adapter._dispatched] == ["e1"]

    @pytest.mark.asyncio
    async def test_home_channel_does_not_capture_specialist_only_mention(self, adapter):
        adapter.home_channel = CHANNEL
        adapter._channel_policy_checked_at[CHANNEL] = float("inf")
        adapter.observe_unaddressed_messages = True
        adapter._observe_unaddressed_message = AsyncMock()
        event = _event("e1", content="@Cosmo handle this", created_at=10)
        event["tags"].append(["p", "b" * 64])

        await self._poll_with(adapter, event)

        assert adapter._dispatched == []
        adapter._observe_unaddressed_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_home_channel_dispatches_when_primary_is_one_of_the_targets(
        self, adapter
    ):
        adapter.home_channel = CHANNEL
        adapter._channel_policy_checked_at[CHANNEL] = float("inf")
        event = _event("e1", content="@Chip and @Cosmo coordinate", created_at=10)
        event["tags"].extend([["p", SELF_PUBKEY], ["p", "b" * 64]])

        await self._poll_with(adapter, event)

        assert [d["message_id"] for d in adapter._dispatched] == ["e1"]

    @pytest.mark.asyncio
    async def test_home_channel_observes_specialist_result_without_dispatch(
        self, adapter
    ):
        adapter.home_channel = CHANNEL
        adapter._channel_policy_checked_at[CHANNEL] = float("inf")
        adapter.observe_unaddressed_messages = True
        adapter._allowed_pubkeys = {OTHER_PUBKEY}
        adapter._observe_unaddressed_message = AsyncMock()
        specialist = "b" * 64

        await self._poll_with(
            adapter,
            _event(
                "e1",
                pubkey=specialist,
                content="Specialist task completed",
                created_at=10,
            ),
        )

        assert adapter._dispatched == []
        adapter._observe_unaddressed_message.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bare_slash_is_ignored_without_primary_agent_opt_in(self, adapter):
        await self._poll_with(adapter, _event("e1", content="/status", created_at=10))
        assert adapter._dispatched == []

    @pytest.mark.asyncio
    async def test_bare_slash_dispatches_for_opted_in_primary_agent(self, adapter):
        adapter.accept_bare_slash_commands = True
        await self._poll_with(adapter, _event("e1", content="/status", created_at=10))
        assert [d["text"] for d in adapter._dispatched] == ["/status"]

    @pytest.mark.asyncio
    async def test_allowlist_blocks_unauthorized(self, adapter):
        adapter._allowed_pubkeys = {"b" * 64}
        await self._poll_with(
            adapter, _event("e1", content="@Chip hello", created_at=10)
        )
        assert adapter._dispatched == []


# ── DM classification via p-tags (issue #68871) ──────────────────────────
#
# `buzz dms list` returns [] on some hosted relays, so DM conversations leak
# in via `channels list` and get seeded chat_type="group".  The adapter must
# reclassify them from the Nostr tags of real traffic: DM messages are
# p-tagged to our own pubkey WITHOUT the text mentioning us, while channel
# messages only ever p-tag us when the text visibly @mentions us.


def _tagged_event(
    event_id,
    channel,
    *,
    content,
    pubkey=OTHER_PUBKEY,
    created_at=1000,
    kind=9,
    p=None,
    reply_to=None,
):
    """Event with the tag shapes observed on a live relay (h/p/e tags)."""
    tags = [["h", channel]]
    if reply_to:
        tags.append(["e", reply_to, "", "reply"])
    if p:
        tags.append(["p", p])
    return {
        "id": event_id,
        "pubkey": pubkey,
        "content": content,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
    }


class TestDmClassification:
    @pytest.fixture
    def adapter(self):
        a = _make_adapter()
        a._dispatched = []

        async def capture(**kwargs):
            a._dispatched.append(kwargs)

        a._dispatch_message = capture
        a._message_handler = AsyncMock()
        # Metadata exactly as `channels list` returns it on the hosted relay.
        a._channel_meta = {
            DM_CHANNEL: {"channel_id": DM_CHANNEL, "name": "DM", "description": ""},
            CHANNEL: {
                "channel_id": CHANNEL,
                "name": "general",
                "description": "General conversation and community updates.",
            },
        }
        a._channel_names = {DM_CHANNEL: "DM", CHANNEL: "general"}
        # Both leaked in as group — the bug under test.
        a._channel_state[DM_CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        a._channel_state[CHANNEL] = {"chat_type": "group", "last_ts": 0, "seen": {}}
        return a

    async def _poll_with(self, adapter, channel, *events):
        cli = _ScriptedCli()
        cli.script("messages", "get", list(events))
        adapter._run_cli = cli
        await adapter._poll_channel(channel)

    @pytest.mark.asyncio
    async def test_unmentioned_ptagged_dm_latches_and_dispatches(self, adapter):
        """The reported bug: a DM without an @mention must dispatch."""
        await self._poll_with(
            adapter,
            DM_CHANNEL,
            _tagged_event(
                "e1", DM_CHANNEL, content="here's a test message", p=SELF_PUBKEY
            ),
        )
        assert adapter._channel_state[DM_CHANNEL]["chat_type"] == "dm"
        assert [d["message_id"] for d in adapter._dispatched] == ["e1"]
        assert adapter._dispatched[0]["chat_type"] == "dm"

    @pytest.mark.asyncio
    async def test_general_reply_ptagging_self_stays_channel(self, adapter):
        """A #general reply to us p-tags our pubkey (observed live) — that
        must NOT reclassify the channel; mention gating still applies."""
        await self._poll_with(
            adapter,
            CHANNEL,
            _tagged_event(
                "e1",
                CHANNEL,
                content="@chip what's up?",
                p=SELF_PUBKEY,
                reply_to="root-event",
            ),
        )
        assert adapter._channel_state[CHANNEL]["chat_type"] == "group"
        # It carried a mention, so it dispatches — but as a group message.
        assert [d["chat_type"] for d in adapter._dispatched] == ["group"]

        # And once the mention is absent, the channel gate drops the message
        # even though the earlier reply p-tagged us.
        await self._poll_with(
            adapter,
            CHANNEL,
            _tagged_event("e2", CHANNEL, content="thanks everyone", created_at=1001),
        )
        assert len(adapter._dispatched) == 1

    @pytest.mark.asyncio
    async def test_channel_like_metadata_blocks_dm_latch_but_keeps_semantic_target(
        self, adapter
    ):
        """A p-tagged work root stays a group and reaches its assignee.

        Coordinator-created roots intentionally omit visible ``@Name`` text,
        so the signed p-tag is both the anti-DM evidence (via real channel
        metadata) and the authoritative agent assignment signal.
        """
        adapter._channel_meta[CHANNEL]["description"] = ""
        adapter._channel_meta[CHANNEL]["name"] = "announcements"
        await self._poll_with(
            adapter,
            CHANNEL,
            _tagged_event("e1", CHANNEL, content="fyi everyone", p=SELF_PUBKEY),
        )
        assert adapter._channel_state[CHANNEL]["chat_type"] == "group"
        assert [item["message_id"] for item in adapter._dispatched] == ["e1"]

    @pytest.mark.asyncio
    async def test_dm_shaped_channel_discovered_when_dms_list_empty(self):
        """Fallback discovery: with `dms list` broken (returns []), a
        DM-shaped `channels list` entry gets watched; real channels not
        already watched are left alone."""
        a = _make_adapter()
        cli = _ScriptedCli()
        cli.script("dms", "list", [])
        cli.script(
            "channels",
            "list",
            [
                {
                    "channel_id": DM_CHANNEL,
                    "name": "DM",
                    "description": "",
                    "created_at": 1,
                },
                {
                    "channel_id": CHANNEL,
                    "name": "general",
                    "description": "General conversation and community updates.",
                    "created_at": 2,
                },
            ],
        )
        a._run_cli = cli
        await a._discover_dms(seed=False)
        # With no explicit channel allow-list every newly joined conversation
        # is watched. The p-tag latch still flips the DM on first real traffic.
        assert a._channel_state[DM_CHANNEL]["chat_type"] == "group"
        assert a._may_reclassify_as_dm(DM_CHANNEL) is True
        assert a._channel_state[CHANNEL]["chat_type"] == "group"
        assert a._may_reclassify_as_dm(CHANNEL) is False


# ── Thread-to-session routing ─────────────────────────────────────────────


class TestThreadRouting:
    def test_top_level_message_stays_in_channel_session(self):
        assert BuzzAdapter._thread_root_id(_event("root")) is None

    def test_titled_stream_root_starts_thread_session(self):
        event = _event("a" * 64)
        event["tags"].append(["subject", "Focused work"])
        assert BuzzAdapter._thread_root_id(event) == "a" * 64

    def test_titled_root_edit_keeps_the_original_thread_session(self):
        event = _event("b" * 64, kind=40003)
        event["tags"].extend(
            [["e", "a" * 64], ["subject", "Renamed focused work"]]
        )
        assert BuzzAdapter._thread_root_id(event) == "a" * 64

    def test_forum_root_with_quoted_event_still_owns_its_session(self):
        event = _event("a" * 64, kind=45001)
        event["tags"].append(["e", "b" * 64])
        assert BuzzAdapter._thread_root_id(event) == "a" * 64

    def test_direct_reply_continues_root_session(self):
        event = _event("reply")
        event["tags"].append(["e", "root", "", "reply"])
        assert BuzzAdapter._thread_root_id(event) == "root"

    def test_nested_reply_prefers_outer_root_marker(self):
        event = _event("nested")
        event["tags"].extend([
            ["e", "root", "", "root"],
            ["e", "reply", "", "reply"],
        ])
        assert BuzzAdapter._thread_root_id(event) == "root"

    def test_quote_ids_are_ordered_deduplicated_and_validated(self):
        first = "1" * 64
        second = "2" * 64
        event = _event("reply")
        event["tags"].extend([
            ["q", first],
            ["q", "not-an-event"],
            ["q", second],
            ["q", first],
        ])
        assert BuzzAdapter._quoted_event_ids(event) == [first, second]

    @pytest.mark.asyncio
    async def test_lossless_long_message_attachment_is_verified_and_hydrated(
        self, monkeypatch
    ):
        adapter = _make_adapter()
        payload = ("@Chip inspect every line\n" + "details\n" * 1024).encode()
        digest = _buzz_mod.hashlib.sha256(payload).hexdigest()
        event = _event(
            "long",
            content="The complete lossless message is attached as buzz-message.md.\n"
            "[buzz-message.md](https://test.relay/media/blob)",
        )
        event["tags"].append([
            "imeta",
            "url https://test.relay/media/blob",
            "m application/octet-stream",
            f"x {digest}",
            f"size {len(payload)}",
            "filename buzz-message.md",
        ])

        async def download(args):
            assert args == [
                "media",
                "get",
                "https://test.relay/media/blob",
                "--output",
                "-",
            ]
            return 0, payload, ""

        monkeypatch.setattr(adapter, "_run_cli_bytes", download)

        hydrated = await adapter._hydrate_long_message_attachment(
            event, event["content"]
        )

        assert hydrated == payload.decode()

    @pytest.mark.asyncio
    async def test_long_message_attachment_never_fetches_cross_origin(
        self, monkeypatch
    ):
        adapter = _make_adapter()
        event = _event(
            "long",
            content="The complete lossless message is attached as buzz-message.md.",
        )
        event["tags"].append([
            "imeta",
            "url https://internal.example/media/blob",
            f"x {'a' * 64}",
            "size 12",
            "filename buzz-message.md",
        ])
        called = False

        async def forbidden(*_args, **_kwargs):
            nonlocal called
            called = True
            raise AssertionError("cross-origin fetch")

        monkeypatch.setattr(adapter, "_run_cli_bytes", forbidden)

        hydrated = await adapter._hydrate_long_message_attachment(
            event, event["content"]
        )

        assert hydrated == event["content"]
        assert called is False

    @pytest.mark.asyncio
    async def test_selected_message_context_is_hydrated_without_changing_content(self):
        adapter = _make_adapter()
        first = "1" * 64
        second = "2" * 64
        cli = _ScriptedCli()
        cli.script(
            "messages",
            "thread",
            [{"id": first, "pubkey": "a" * 64, "content": "first full message"}],
        )
        cli.script(
            "messages",
            "thread",
            [{"id": second, "pubkey": "b" * 64, "content": "second full message"}],
        )
        adapter._run_cli = cli
        event = _event("reply")
        event["tags"].extend([["q", first], ["q", second]])

        context = await adapter._load_quoted_context(CHANNEL, event)

        assert "first full message" in context
        assert "second full message" in context
        assert len(cli.calls) == 2
        assert all("--depth-limit" in args for args, _stdin in cli.calls)

    @pytest.mark.asyncio
    async def test_dispatch_carries_stable_thread_and_message_anchor(self):
        adapter = _make_adapter({"home_channel": CHANNEL})
        adapter._message_handler = AsyncMock()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "group",
            "last_ts": 0,
            "seen": {},
        }
        captured = []

        async def capture(**kwargs):
            captured.append(kwargs)

        adapter._dispatch_message = capture
        nested = _event("nested", content="continue", created_at=10)
        nested["tags"].extend([
            ["e", "root", "", "root"],
            ["e", "reply", "", "reply"],
        ])

        await adapter._handle_event(CHANNEL, adapter._channel_state[CHANNEL], nested)

        assert captured[0]["thread_id"] == "root"
        assert captured[0]["message_id"] == "nested"
        assert captured[0]["raw_event"] == nested

    @pytest.mark.asyncio
    async def test_dispatch_auto_loads_all_skills_bound_to_channel(self):
        adapter = _make_adapter({
            "channel_skill_bindings": [
                {
                    "id": CHANNEL,
                    "skills": ["research", "summarize", "research"],
                }
            ]
        })
        adapter._message_handler = AsyncMock()
        adapter.handle_message = AsyncMock()

        await adapter._dispatch_message(
            text="look into this",
            chat_id=CHANNEL,
            chat_type="group",
            user_id=OTHER_PUBKEY,
            user_name="Alice",
            message_id="event-1",
            created_at=10,
            thread_id="root-1",
        )

        dispatched = adapter.handle_message.await_args.args[0]
        assert dispatched.auto_skill == ["research", "summarize"]


class TestLiveActivity:
    @pytest.mark.asyncio
    async def test_typing_heartbeat_is_thread_scoped_and_non_blocking(self):
        adapter = _make_adapter()
        adapter.cli_path = "/fake/buzz"
        cli = _ScriptedCli()
        adapter._run_cli = cli

        await adapter.send_typing(CHANNEL, metadata={"thread_id": "a" * 64})
        await asyncio.gather(*adapter._typing_publish_tasks.values())

        assert cli.calls == [
            (
                [
                    "messages",
                    "typing",
                    "--channel",
                    CHANNEL,
                    "--thread",
                    "a" * 64,
                ],
                None,
            )
        ]

    @pytest.mark.asyncio
    async def test_structured_tool_lifecycle_uses_owner_visible_activity(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        adapter._run_cli = cli

        await adapter.publish_tool_started(
            CHANNEL,
            "call-1",
            "terminal",
            {"command": "cargo test"},
            session_id="session-1",
            turn_id="turn-1",
        )
        await adapter.publish_tool_completed(
            CHANNEL,
            "call-1",
            "terminal",
            session_id="session-1",
            turn_id="turn-1",
        )

        assert [call[0][:2] for call in cli.calls] == [
            ["agents", "activity"],
            ["agents", "activity"],
        ]
        started = json.loads(cli.calls[0][1])
        completed = json.loads(cli.calls[1][1])
        assert started["params"]["update"] == {
            "sessionUpdate": "tool_call",
            "toolCallId": "call-1",
            "title": "terminal",
            "toolName": "terminal",
            "status": "executing",
            "args": {"command": "cargo test"},
        }
        assert completed["params"]["update"]["status"] == "completed"
        assert "--session" in cli.calls[0][0]
        assert "--turn" in cli.calls[0][0]

    @pytest.mark.asyncio
    async def test_structured_activity_carries_explicit_thread_scope(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        adapter._run_cli = cli
        thread_id = "a" * 64

        await adapter.publish_tool_started(
            CHANNEL,
            "call-thread",
            "read_file",
            {"path": "README.md"},
            session_id="hermes-session",
            turn_id="turn-thread",
            metadata={"thread_id": thread_id},
        )
        args = cli.calls[0][0]

        assert args[args.index("--thread") + 1] == thread_id
        assert args[args.index("--session") + 1] == thread_id

    @pytest.mark.asyncio
    async def test_channel_root_activity_omits_thread_scope(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        adapter._run_cli = cli

        await adapter.publish_tool_started(
            CHANNEL,
            "call-root",
            "terminal",
            session_id="hermes-session",
            turn_id="turn-root",
        )

        assert "--thread" not in cli.calls[0][0]


# ── Sending ───────────────────────────────────────────────────────────────


class TestBuzzAdapterSend:
    def test_prefers_fresh_final_streaming(self):
        adapter = _make_adapter()

        assert adapter.prefers_fresh_final_streaming("complete answer") is True

    def test_requires_explicit_finalize_for_identical_cursorless_last_frame(self):
        adapter = _make_adapter()

        assert adapter.REQUIRES_EDIT_FINALIZE is True

    @pytest.mark.asyncio
    async def test_send_success_via_stdin(self):
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "group",
            "last_ts": 0,
            "seen": {},
        }
        cli = _ScriptedCli()
        cli.script(
            "messages", "send", {"accepted": True, "event_id": "evt123", "message": ""}
        )
        adapter._run_cli = cli

        result = await adapter.send(CHANNEL, "hello **markdown**")
        assert result.success is True
        assert result.message_id == "evt123"

        args, stdin_text = cli.calls[0]
        assert args[:2] == ["messages", "send"]
        assert args[args.index("--channel") + 1] == CHANNEL
        # Content travels via stdin (--content -), never argv
        assert args[args.index("--content") + 1] == "-"
        assert stdin_text == "hello **markdown**"
        # Our own event id is marked seen for echo suppression
        assert "evt123" in adapter._channel_state[CHANNEL]["seen"]

    @pytest.mark.asyncio
    async def test_completed_send_marks_only_final_response(self):
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "group",
            "last_ts": 0,
            "seen": {},
        }
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "final"})
        adapter._run_cli = cli

        await adapter.send(CHANNEL, "done", metadata={"notify": True})

        assert "--final-response" in cli.calls[0][0]

    @pytest.mark.asyncio
    async def test_interim_send_never_marks_final_response(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "partial"})
        adapter._run_cli = cli

        await adapter.send(
            CHANNEL,
            "working",
            metadata={"notify": True, "_interim_send": True},
        )

        assert "--final-response" not in cli.calls[0][0]

    @pytest.mark.asyncio
    async def test_top_level_group_response_does_not_create_thread(self):
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "group",
            "last_ts": 0,
            "seen": {},
        }
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "evt-top"})
        adapter._run_cli = cli

        await adapter.send(CHANNEL, "top-level response", reply_to="user-message")

        args, _stdin = cli.calls[0]
        assert "--reply-to" not in args

    @pytest.mark.asyncio
    async def test_thread_root_wins_over_nested_reply_anchor(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "evt124"})
        adapter._run_cli = cli

        await adapter.send(
            CHANNEL,
            "flat response",
            reply_to="nested-user-reply",
            metadata={"thread_id": "outer-root"},
        )

        args, _stdin = cli.calls[0]
        assert args[args.index("--reply-to") + 1] == "outer-root"

    @pytest.mark.asyncio
    async def test_forum_response_uses_comment_kind_and_canonical_root(self):
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "group",
            "forum": True,
            "last_ts": 0,
            "seen": {},
        }
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "forum-reply"})
        adapter._run_cli = cli
        post_id = "a" * 64

        await adapter.send(
            CHANNEL,
            "forum response",
            reply_to="nested-comment",
            metadata={"thread_id": post_id},
        )

        args, _stdin = cli.calls[0]
        assert args[args.index("--reply-to") + 1] == post_id
        assert args[args.index("--kind") + 1] == "45003"

    @pytest.mark.asyncio
    async def test_dm_style_send_stays_top_level_without_explicit_thread(self):
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "dm",
            "last_ts": 0,
            "seen": {},
        }
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "evt125"})
        adapter._run_cli = cli

        await adapter.send(
            CHANNEL,
            "dm response",
            reply_to="dm-message",
            metadata=None,
        )

        args, _stdin = cli.calls[0]
        assert "--reply-to" not in args

    @pytest.mark.asyncio
    async def test_edit_streams_content_via_stdin(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("messages", "edit", {"accepted": True, "event_id": "edit-1"})
        adapter._run_cli = cli

        result = await adapter.edit_message(
            CHANNEL,
            "original-message",
            "partial response",
        )

        assert result.success is True
        assert result.message_id == "original-message"
        args, stdin_text = cli.calls[0]
        assert args == [
            "messages",
            "edit",
            "--event",
            "original-message",
            "--content",
            "-",
        ]
        assert stdin_text == "partial response"

    @pytest.mark.asyncio
    async def test_delete_message_publishes_buzz_deletion_event(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("messages", "delete", {"accepted": True, "event_id": "delete-1"})
        adapter._run_cli = cli

        deleted = await adapter.delete_message(CHANNEL, "original-message")

        assert deleted is True
        assert cli.calls == [
            (["messages", "delete", "--event", "original-message"], None)
        ]

    @pytest.mark.asyncio
    async def test_send_image_local_file_uses_file_flag(self, tmp_path):
        img = tmp_path / "shot.png"
        img.write_bytes(b"\x89PNG fake")
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script(
            "messages", "send", {"accepted": True, "event_id": "evt126", "message": ""}
        )
        adapter._run_cli = cli
        result = await adapter.send_image(CHANNEL, str(img), caption="screenshot")
        assert result.success is True
        args, _stdin = cli.calls[0]
        assert args[args.index("--file") + 1] == str(img)

    @pytest.mark.asyncio
    async def test_forum_image_reply_uses_comment_kind_and_root(self, tmp_path):
        img = tmp_path / "forum-shot.png"
        img.write_bytes(b"\x89PNG fake")
        adapter = _make_adapter()
        adapter._channel_state[CHANNEL] = {
            "chat_type": "group",
            "forum": True,
            "last_ts": 0,
            "seen": {},
        }
        cli = _ScriptedCli()
        cli.script("messages", "send", {"accepted": True, "event_id": "image-reply"})
        adapter._run_cli = cli
        post_id = "a" * 64

        result = await adapter.send_image(
            CHANNEL,
            str(img),
            caption="evidence",
            reply_to="nested-comment",
            metadata={"thread_id": post_id},
        )

        assert result.success is True
        args, _stdin = cli.calls[0]
        assert args[args.index("--reply-to") + 1] == post_id
        assert args[args.index("--kind") + 1] == "45003"


# ── Lifecycle ─────────────────────────────────────────────────────────────


class TestBuzzAdapterLifecycle:
    @pytest.mark.asyncio
    async def test_presence_uses_cli_and_tracks_announced_state(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("users", "set-presence", {"accepted": True})
        adapter._run_cli = cli

        assert await adapter._publish_presence("online") is True
        assert adapter._presence_announced is True
        assert cli.calls[-1][0] == [
            "users",
            "set-presence",
            "--status",
            "online",
        ]

        assert await adapter._publish_presence("offline") is True
        assert adapter._presence_announced is False
        assert cli.calls[-1][0] == [
            "users",
            "set-presence",
            "--status",
            "offline",
        ]

    @pytest.mark.asyncio
    async def test_disconnect_cancels_heartbeat_and_publishes_offline(self):
        adapter = _make_adapter()
        cli = _ScriptedCli()
        cli.script("users", "set-presence", {"accepted": True})
        adapter._run_cli = cli
        adapter._presence_announced = True
        adapter._presence_task = asyncio.create_task(asyncio.sleep(3600))

        await adapter.disconnect()

        assert adapter._presence_task is None
        assert adapter._presence_announced is False
        assert cli.calls[-1][0] == [
            "users",
            "set-presence",
            "--status",
            "offline",
        ]

    @pytest.mark.asyncio
    async def test_disconnect_releases_scoped_lock(self, monkeypatch):
        """The identity lock taken in connect() must be released on disconnect."""
        import gateway.status as gateway_status

        released = []
        monkeypatch.setattr(
            gateway_status,
            "release_scoped_lock",
            lambda platform, key: released.append((platform, key)),
        )
        adapter = _make_adapter()
        adapter._lock_key = "wss://relay.example:" + SELF_PUBKEY
        await adapter.disconnect()
        assert released == [("buzz", "wss://relay.example:" + SELF_PUBKEY)]
        assert adapter._lock_key is None

    @pytest.mark.asyncio
    async def test_connect_fails_when_identity_lock_held(self, monkeypatch):
        """A second profile using the same relay+pubkey must fail fast."""
        import gateway.status as gateway_status

        monkeypatch.setattr(
            gateway_status, "acquire_scoped_lock", lambda platform, key: False
        )
        adapter = _make_adapter()
        adapter.cli_path = "/fake/buzz"
        monkeypatch.setattr(
            _buzz_mod, "_resolve_private_key", lambda extra=None: "nsec1test"
        )
        cli = _ScriptedCli()
        cli.script(
            "users",
            "get",
            [{"pubkey": SELF_PUBKEY, "display_name": "Chip"}],
        )
        adapter._run_cli = cli
        assert await adapter.connect() is False
        assert adapter._lock_key is None


# ── Credentials / requirements ────────────────────────────────────────────


class TestCredentialResolution:
    def test_env_key_wins(self, monkeypatch):
        monkeypatch.setenv("BUZZ_PRIVATE_KEY", "nsec1fromenv")
        assert _resolve_private_key() == "nsec1fromenv"

    def test_credentials_file_fallback(self, monkeypatch, tmp_path):
        creds = tmp_path / "agent_credentials.json"
        creds.write_text(
            json.dumps({"nsec": "nsec1fromfile", "npub": "npub1x"}), encoding="utf-8"
        )
        monkeypatch.setenv("BUZZ_CREDENTIALS_FILE", str(creds))
        assert _resolve_private_key() == "nsec1fromfile"


# ── Env enablement / registration / standalone send ──────────────────────


class TestEnvEnablement:
    def test_returns_none_when_unconfigured(self):
        assert _env_enablement() is None


class TestBuzzPluginRegistration:
    def test_register_platform_contract(self):
        from gateway.platform_registry import platform_registry

        platform_registry.unregister("buzz")
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()
        kwargs = ctx.register_platform.call_args.kwargs
        assert kwargs["name"] == "buzz"
        assert kwargs["cron_deliver_env_var"] == "BUZZ_HOME_CHANNEL"
        assert kwargs["allowed_users_env"] == "BUZZ_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "BUZZ_ALLOW_ALL_USERS"
        assert callable(kwargs["standalone_sender_fn"])
        assert callable(kwargs["env_enablement_fn"])
        assert set(kwargs["required_env"]) == {"BUZZ_RELAY_URL", "BUZZ_PRIVATE_KEY"}


class TestStandaloneSend:
    @pytest.mark.asyncio
    async def test_standalone_send_success(self, monkeypatch, tmp_path):
        from gateway.config import PlatformConfig

        fake_cli = tmp_path / "buzz"
        fake_cli.write_text("#!/bin/sh\n", encoding="utf-8")
        monkeypatch.setenv("BUZZ_RELAY_URL", "https://r")
        monkeypatch.setenv("BUZZ_PRIVATE_KEY", "nsec1x")
        monkeypatch.setenv("BUZZ_CLI_PATH", str(fake_cli))

        captured = {}

        async def fake_exec(
            cli_path, args, *, relay_url, private_key, input_text=None, timeout=30.0
        ):
            captured.update(
                cli_path=cli_path, args=args, relay_url=relay_url, input_text=input_text
            )
            return (
                0,
                json.dumps({"accepted": True, "event_id": "evt-cron", "message": ""}),
                "",
            )

        monkeypatch.setattr(_buzz_mod, "_exec_buzz", fake_exec)

        result = await _standalone_send(
            PlatformConfig(enabled=True, extra={}), CHANNEL, "cron says hi"
        )
        assert result == {"success": True, "message_id": "evt-cron"}
        assert captured["args"][:2] == ["messages", "send"]
        assert captured["input_text"] == "cron says hi"
        # The private key must never be part of argv
        assert all("nsec1x" not in str(a) for a in captured["args"])
