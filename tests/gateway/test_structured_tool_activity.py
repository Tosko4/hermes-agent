"""Generic gateway projection of ID-correlated tool activity."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from gateway.run import TurnRunner
from gateway.turn_context import TurnContext


@pytest.mark.asyncio
async def test_turn_runner_projects_tool_start_and_completion_out_of_band():
    adapter = MagicMock()
    adapter.publish_tool_started = AsyncMock()
    adapter.publish_tool_completed = AsyncMock()
    ctx = TurnContext(
        source=SimpleNamespace(chat_id="channel-1"),
        _run_still_current=lambda: True,
        _structured_tool_activity_adapter=adapter,
        session_id="session-internal",
        event_message_id="turn-1",
        _status_thread_metadata={"thread_id": "thread-root"},
        _voice_ack_loop=asyncio.get_running_loop(),
    )
    runner = TurnRunner(MagicMock(), ctx)

    runner.combined_tool_start_callback("call-1", "terminal", {"command": "pwd"})
    runner.combined_tool_complete_callback("call-1", "terminal", {}, "done")
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    adapter.publish_tool_started.assert_awaited_once_with(
        chat_id="channel-1",
        tool_call_id="call-1",
        tool_name="terminal",
        args={"command": "pwd"},
        session_id="session-internal",
        turn_id="turn-1",
        metadata={"thread_id": "thread-root"},
    )
    adapter.publish_tool_completed.assert_awaited_once_with(
        chat_id="channel-1",
        tool_call_id="call-1",
        tool_name="terminal",
        is_error=False,
        session_id="session-internal",
        turn_id="turn-1",
        metadata={"thread_id": "thread-root"},
    )
