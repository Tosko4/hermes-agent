"""Security and lifecycle tests for the Buzz mobile terminal broker."""

import asyncio
import time
import uuid

import pytest

from plugins.platforms.buzz.terminal_broker import (
    PROTOCOL_KIND,
    PROTOCOL_VERSION,
    TerminalBroker,
    _clean_terminal_text,
    _guard_command_sync,
)

OWNER = "a" * 64
AGENT = "b" * 64
CHANNEL = "ccc2bc1a-7a82-5a8f-8c4e-57a070cbe7cd"


class Harness:
    def __init__(self, guard=lambda _command, _key: {"approved": True}):
        self.telemetry = []

        async def decrypt(event):
            return {
                "author": event.get("author", OWNER),
                "event_id": event["event_id"],
                "payload": event["payload"],
            }

        async def publish(payload):
            self.telemetry.append(payload)
            return True

        self.broker = TerminalBroker(
            agent_pubkey=AGENT,
            allowed_users={OWNER},
            channel_id=CHANNEL,
            cwd="/tmp",
            decrypt_event=decrypt,
            publish_telemetry=publish,
            guard_command=guard,
            idle_ttl_secs=60,
            absolute_ttl_secs=120,
        )

    async def start(self):
        await self.broker.start()

    async def stop(self):
        await self.broker.stop()

    async def control(
        self, action, *, session_id="", data=None, request_id=None, **event
    ):
        request_id = request_id or str(uuid.uuid4())
        payload = {
            "kind": PROTOCOL_KIND,
            "version": PROTOCOL_VERSION,
            "direction": "control",
            "action": action,
            "request_id": request_id,
            "sent_at": int(time.time()),
            "channel_id": CHANNEL,
            "data": data or {},
        }
        if action != "open":
            payload["session_id"] = session_id
        await self.broker.handle_event({
            "event_id": event.get("event_id", uuid.uuid4().hex * 2),
            "author": event.get("author", OWNER),
            "payload": payload,
        })
        return request_id

    async def wait_for(self, action, timeout=3):
        async def find():
            while True:
                for payload in self.telemetry:
                    if payload["action"] == action:
                        return payload
                await asyncio.sleep(0.01)

        return await asyncio.wait_for(find(), timeout=timeout)


@pytest.mark.asyncio
async def test_safe_command_runs_in_persistent_shell_and_reports_cwd():
    harness = Harness()
    await harness.start()
    try:
        await harness.control("open")
        opened = await harness.wait_for("opened")
        session_id = opened["session_id"]

        await harness.control(
            "command",
            session_id=session_id,
            data={"command": "cd /tmp && printf 'hello-terminal'"},
        )
        completed = await harness.wait_for("completed")
        output = "".join(
            item["data"]["text"]
            for item in harness.telemetry
            if item["action"] == "output"
        )
        assert "hello-terminal" in output
        assert completed["data"] == {"exit_code": 0, "cwd": "/tmp"}
        assert harness.broker.sessions[session_id].process.poll() is None
    finally:
        process = next(iter(harness.broker.sessions.values())).process
        await harness.stop()
        assert process.poll() is not None


@pytest.mark.asyncio
async def test_blocked_command_never_reaches_pty(monkeypatch):
    harness = Harness(
        guard=lambda _command, _key: {
            "approved": False,
            "message": "BLOCKED by test guard",
        }
    )
    await harness.start()
    try:
        await harness.control("open")
        session_id = (await harness.wait_for("opened"))["session_id"]
        wrote = False

        def forbidden_write(*_args):
            nonlocal wrote
            wrote = True
            raise AssertionError("blocked command reached PTY")

        monkeypatch.setattr(harness.broker, "_write_command", forbidden_write)
        await harness.control(
            "command", session_id=session_id, data={"command": "touch /tmp/nope"}
        )
        rejected = await harness.wait_for("command_rejected")
        assert "BLOCKED" in rejected["data"]["message"]
        assert wrote is False
    finally:
        await harness.stop()


@pytest.mark.asyncio
async def test_wrong_owner_channel_and_replay_cannot_open_session():
    harness = Harness()
    await harness.start()
    try:
        request_id = str(uuid.uuid4())
        await harness.control("open", author="c" * 64, request_id=request_id)
        assert harness.broker.sessions == {}

        payload_request = str(uuid.uuid4())
        event_id = uuid.uuid4().hex * 2
        await harness.control("open", request_id=payload_request, event_id=event_id)
        await harness.control("open", request_id=payload_request, event_id=event_id)
        assert len(harness.broker.sessions) == 1

        bad_payload = {
            "kind": PROTOCOL_KIND,
            "version": PROTOCOL_VERSION,
            "direction": "control",
            "action": "open",
            "request_id": str(uuid.uuid4()),
            "sent_at": int(time.time()),
            "channel_id": str(uuid.uuid4()),
            "data": {},
        }
        await harness.broker.handle_event({
            "event_id": uuid.uuid4().hex * 2,
            "author": OWNER,
            "payload": bad_payload,
        })
        assert len(harness.broker.sessions) == 1
    finally:
        await harness.stop()


@pytest.mark.asyncio
async def test_approval_response_is_bound_to_exact_pending_id(monkeypatch):
    harness = Harness()
    await harness.start()
    try:
        await harness.control("open")
        session_id = (await harness.wait_for("opened"))["session_id"]
        session = harness.broker.sessions[session_id]
        session.approval_request_id = "approval-1"
        resolved = []

        def resolve(session_key, choice, *, request_id):
            resolved.append((session_key, choice, request_id))
            return 1

        monkeypatch.setattr("tools.approval.resolve_gateway_approval", resolve)
        await harness.control(
            "approval_response",
            session_id=session_id,
            data={"approval_id": "wrong", "choice": "once"},
        )
        assert resolved == []
        assert any(item["action"] == "error" for item in harness.telemetry)

        await harness.control(
            "approval_response",
            session_id=session_id,
            data={"approval_id": "approval-1", "choice": "once"},
        )
        assert resolved == [(session.approval_session_key, "once", "approval-1")]
    finally:
        await harness.stop()


@pytest.mark.asyncio
async def test_dangerous_command_waits_for_matching_mobile_approval(monkeypatch):
    from tools import approval

    monkeypatch.setattr(
        approval, "_get_approval_config", lambda: {"mode": "manual", "timeout": 3}
    )
    harness = Harness(guard=_guard_command_sync)
    await harness.start()
    try:
        await harness.control("open")
        session_id = (await harness.wait_for("opened"))["session_id"]
        command_request = await harness.control(
            "command",
            session_id=session_id,
            data={"command": "rm -rf /tmp/buzz-terminal-approval-test-nonexistent"},
        )
        approval_prompt = await harness.wait_for("approval_required")
        assert approval_prompt["request_id"] == command_request
        assert not any(item["action"] == "completed" for item in harness.telemetry)

        await harness.control(
            "approval_response",
            session_id=session_id,
            data={
                "approval_id": approval_prompt["data"]["approval_id"],
                "choice": "once",
            },
        )
        completed = await harness.wait_for("completed")
        assert completed["request_id"] == command_request
        assert completed["data"]["exit_code"] == 0
    finally:
        await harness.stop()


def test_hardline_guard_blocks_even_in_gateway_context():
    result = _guard_command_sync("rm -rf /", f"buzz-terminal:{uuid.uuid4()}")
    assert result["approved"] is False
    assert "BLOCKED" in result["message"]


def test_terminal_output_is_rendered_as_inert_text():
    raw = "safe\x1b]52;c;ZXZpbA==\x07\x1b[31mred\x1b[0m\x00\n"
    assert _clean_terminal_text(raw) == "safered\n"
