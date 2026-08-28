"""Owner-private, approval-gated PTY sessions transported over Buzz observer frames."""

from __future__ import annotations

import asyncio
import base64
import codecs
import errno
import hashlib
import json
import logging
import os
import re
import select
import shutil
import signal
import subprocess
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

if os.name == "posix":
    import fcntl
    import pty
    import termios

logger = logging.getLogger(__name__)

PROTOCOL_KIND = "terminal_protocol"
PROTOCOL_VERSION = 1
MAX_COMMAND_BYTES = 16 * 1024
MAX_SESSIONS = 2
MAX_REPLAY_ENTRIES = 1_000
MAX_PENDING_OUTPUT_BYTES = 512 * 1024
MAX_HISTORY_BYTES = 512 * 1024
OUTPUT_CHUNK_BYTES = 32 * 1024
OUTPUT_INTERVAL_SECS = 0.05
DEFAULT_IDLE_TTL_SECS = 15 * 60
DEFAULT_ABSOLUTE_TTL_SECS = 2 * 60 * 60
MAX_CLOCK_SKEW_SECS = 300
MIN_COLS = 20
MAX_COLS = 400
MIN_ROWS = 5
MAX_ROWS = 200

_OSC_RE = re.compile(r"\x1b\].*?(?:\x07|\x1b\\)", re.DOTALL)
_CSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_ESC_RE = re.compile(r"\x1b[@-_][0-?]*")

DecryptEvent = Callable[[dict], Awaitable[dict]]
PublishTelemetry = Callable[[dict], Awaitable[bool]]


def _clean_terminal_text(value: str) -> str:
    """Return inert text: preserve layout, remove terminal-control sequences."""
    value = _OSC_RE.sub("", value)
    value = _CSI_RE.sub("", value)
    value = _ESC_RE.sub("", value)
    return "".join(ch for ch in value if ch in "\n\r\t" or ord(ch) >= 32)


def _canonical_uuid(value: Any, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be a UUID")
    parsed = uuid.UUID(value)
    if str(parsed) != value.lower():
        raise ValueError(f"{field_name} must be a canonical UUID")
    return str(parsed)


def _bounded_dimension(value: Any, minimum: int, maximum: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _guard_command_sync(command: str, session_key: str) -> dict:
    """Run the canonical Hermes guard in a gateway-bound context."""
    from gateway.session_context import clear_session_vars, set_session_vars
    from tools.approval import (
        check_all_command_guards,
        reset_current_session_key,
        set_current_session_key,
    )

    context_tokens = set_session_vars(
        platform="buzz",
        source="buzz-terminal",
        chat_type="terminal",
        session_key=session_key,
        async_delivery=True,
        cron_session="",
    )
    approval_token = set_current_session_key(session_key)
    try:
        return check_all_command_guards(command, "local")
    finally:
        reset_current_session_key(approval_token)
        clear_session_vars(context_tokens)


@dataclass
class _Session:
    session_id: str
    owner: str
    channel_id: str
    process: subprocess.Popen
    master_fd: int
    cwd: str
    created_at: float = field(default_factory=time.monotonic)
    last_activity: float = field(default_factory=time.monotonic)
    busy: bool = False
    command_request_id: str = ""
    approval_request_id: str = ""
    marker: bytes = b""
    read_buffer: bytearray = field(default_factory=bytearray)
    pending_output: bytearray = field(default_factory=bytearray)
    output_event: asyncio.Event = field(default_factory=asyncio.Event)
    output_task: Optional[asyncio.Task] = None
    command_task: Optional[asyncio.Task] = None
    completion: Optional[tuple[str, int, str]] = None
    next_seq: int = 1
    history: OrderedDict[int, str] = field(default_factory=OrderedDict)
    history_bytes: int = 0
    output_truncated: bool = False
    decoder: Any = field(
        default_factory=lambda: codecs.getincrementaldecoder("utf-8")(errors="replace")
    )

    @property
    def approval_session_key(self) -> str:
        return f"buzz-terminal:{self.owner[:16]}:{self.session_id}"


class TerminalBroker:
    """Own bounded PTYs and route their encrypted control/telemetry protocol."""

    def __init__(
        self,
        *,
        agent_pubkey: str,
        allowed_users: set[str],
        channel_id: str,
        decrypt_event: DecryptEvent,
        publish_telemetry: PublishTelemetry,
        cwd: str = "",
        idle_ttl_secs: float = DEFAULT_IDLE_TTL_SECS,
        absolute_ttl_secs: float = DEFAULT_ABSOLUTE_TTL_SECS,
        guard_command: Callable[[str, str], dict] = _guard_command_sync,
    ) -> None:
        if os.name != "posix":
            raise RuntimeError("Buzz server terminal currently requires a POSIX host")
        self.agent_pubkey = agent_pubkey.lower()
        self.allowed_users = {entry.lower() for entry in allowed_users}
        self.channel_id = _canonical_uuid(channel_id, "terminal channel")
        if not self.allowed_users:
            raise ValueError(
                "terminal_allowed_users must contain an exact owner pubkey"
            )
        if any(
            len(value) != 64 or not all(c in "0123456789abcdef" for c in value)
            for value in self.allowed_users
        ):
            raise ValueError(
                "terminal_allowed_users entries must be 64-character hex pubkeys"
            )
        start_cwd = Path(cwd).expanduser() if cwd else Path.home()
        if not start_cwd.is_dir():
            raise ValueError(f"terminal cwd is not a directory: {start_cwd}")
        self.cwd = str(start_cwd.resolve())
        self.decrypt_event = decrypt_event
        self.publish_telemetry = publish_telemetry
        self.idle_ttl_secs = max(float(idle_ttl_secs), 1.0)
        self.absolute_ttl_secs = max(float(absolute_ttl_secs), self.idle_ttl_secs)
        self.guard_command = guard_command
        self.sessions: dict[str, _Session] = {}
        self._seen_event_ids: OrderedDict[str, None] = OrderedDict()
        self._seen_request_ids: OrderedDict[str, None] = OrderedDict()
        self._reaper_task: Optional[asyncio.Task] = None
        self._closed = False
        self._base64_path = shutil.which("base64")
        if not self._base64_path:
            raise RuntimeError(
                "the host base64 utility is required for terminal commands"
            )

    async def start(self) -> None:
        if self._reaper_task is None or self._reaper_task.done():
            self._closed = False
            self._reaper_task = asyncio.create_task(self._reaper_loop())

    async def stop(self) -> None:
        self._closed = True
        if self._reaper_task and not self._reaper_task.done():
            self._reaper_task.cancel()
            await asyncio.gather(self._reaper_task, return_exceptions=True)
        self._reaper_task = None
        for session_id in list(self.sessions):
            await self._close_session(
                session_id, reason="gateway_stopped", notify=False
            )

    async def handle_event(self, event: dict) -> None:
        """Verify/decrypt one event and dispatch it without blocking the WS reader."""
        try:
            decoded = await self.decrypt_event(event)
            envelope, payload = self._validate_decoded(decoded)
        except Exception as exc:
            logger.warning("Buzz terminal rejected a control frame: %s", exc)
            return

        event_id = envelope["event_id"]
        request_id = payload["request_id"]
        if self._remember_once(self._seen_event_ids, event_id) is False:
            return
        if self._remember_once(self._seen_request_ids, request_id) is False:
            return

        action = payload["action"]
        data = payload.get("data") or {}
        session_id = payload.get("session_id") or ""
        owner = envelope["author"]
        try:
            if action == "open":
                await self._open(owner, request_id, data)
            elif action == "approval_response":
                await self._approval_response(owner, session_id, request_id, data)
            else:
                session = self._require_session(owner, session_id)
                session.last_activity = time.monotonic()
                if action == "command":
                    self._start_command(session, request_id, data)
                elif action == "resize":
                    await self._resize(session, request_id, data)
                elif action == "ack":
                    await self._ack(session, data)
                elif action == "reconnect":
                    await self._reconnect(session, request_id, data)
                elif action == "interrupt":
                    await self._interrupt(session, request_id)
                elif action == "close":
                    await self._close_session(session.session_id, reason="owner_closed")
                else:
                    raise ValueError(f"unsupported terminal action: {action}")
        except Exception as exc:
            logger.warning(
                "Buzz terminal action rejected owner=%s session=%s action=%s: %s",
                owner[:12],
                session_id[:12],
                action,
                exc,
            )
            await self._emit(
                action="error",
                request_id=request_id,
                session_id=session_id,
                data={"code": "invalid_request", "message": str(exc)},
            )

    def _validate_decoded(self, decoded: dict) -> tuple[dict, dict]:
        if not isinstance(decoded, dict) or not isinstance(
            decoded.get("payload"), dict
        ):
            raise ValueError("decrypted observer output is malformed")
        author = str(decoded.get("author") or "").lower()
        if author not in self.allowed_users:
            raise PermissionError("control author is not terminal-allowed")
        event_id = str(decoded.get("event_id") or "").lower()
        if len(event_id) != 64 or not all(c in "0123456789abcdef" for c in event_id):
            raise ValueError("verified event id is malformed")
        payload = decoded["payload"]
        if len(json.dumps(payload, separators=(",", ":")).encode("utf-8")) > 65_535:
            raise ValueError("terminal control payload is too large")
        if (
            payload.get("kind") != PROTOCOL_KIND
            or payload.get("version") != PROTOCOL_VERSION
        ):
            raise ValueError("unsupported terminal protocol")
        if payload.get("direction") != "control":
            raise ValueError("terminal payload has wrong direction")
        if payload.get("channel_id") != self.channel_id:
            raise PermissionError("terminal payload names another channel")
        payload["request_id"] = _canonical_uuid(payload.get("request_id"), "request_id")
        sent_at = payload.get("sent_at")
        if isinstance(sent_at, bool) or not isinstance(sent_at, int):
            raise ValueError("sent_at must be integer Unix seconds")
        if abs(int(time.time()) - sent_at) > MAX_CLOCK_SKEW_SECS:
            raise ValueError("terminal control payload is outside the freshness window")
        action = payload.get("action")
        if action not in {
            "open",
            "command",
            "resize",
            "ack",
            "reconnect",
            "interrupt",
            "close",
            "approval_response",
        }:
            raise ValueError("unsupported terminal action")
        if action != "open":
            payload["session_id"] = _canonical_uuid(
                payload.get("session_id"), "session_id"
            )
        if not isinstance(payload.get("data") or {}, dict):
            raise ValueError("terminal data must be an object")
        return {"author": author, "event_id": event_id}, payload

    @staticmethod
    def _remember_once(cache: OrderedDict[str, None], value: str) -> bool:
        if value in cache:
            return False
        cache[value] = None
        while len(cache) > MAX_REPLAY_ENTRIES:
            cache.popitem(last=False)
        return True

    def _require_session(self, owner: str, session_id: str) -> _Session:
        session = self.sessions.get(session_id)
        if (
            session is None
            or session.owner != owner
            or session.channel_id != self.channel_id
        ):
            raise PermissionError(
                "terminal session does not belong to this owner/channel"
            )
        return session

    async def _open(self, owner: str, request_id: str, data: dict) -> None:
        owner_sessions = [
            session for session in self.sessions.values() if session.owner == owner
        ]
        if len(owner_sessions) >= MAX_SESSIONS:
            raise RuntimeError(f"at most {MAX_SESSIONS} terminal sessions are allowed")
        cols = _bounded_dimension(data.get("cols", 100), MIN_COLS, MAX_COLS, "cols")
        rows = _bounded_dimension(data.get("rows", 30), MIN_ROWS, MAX_ROWS, "rows")
        session = self._spawn_session(owner, cols, rows)
        self.sessions[session.session_id] = session
        asyncio.get_running_loop().add_reader(
            session.master_fd, self._on_readable, session.session_id
        )
        session.output_task = asyncio.create_task(self._output_loop(session))
        await self._emit(
            action="opened",
            request_id=request_id,
            session_id=session.session_id,
            data={"cwd": session.cwd, "cols": cols, "rows": rows},
        )

    def _spawn_session(self, owner: str, cols: int, rows: int) -> _Session:
        from tools.environments.local import build_subprocess_env

        master_fd, slave_fd = pty.openpty()
        try:
            attrs = termios.tcgetattr(slave_fd)
            attrs[3] &= ~termios.ECHO
            termios.tcsetattr(slave_fd, termios.TCSANOW, attrs)
            self._set_window_size(master_fd, cols, rows)
            env = build_subprocess_env(scrub_secrets=True)
            env.update({
                "TERM": "dumb",
                "NO_COLOR": "1",
                "CLICOLOR": "0",
                "PS1": "",
                "PROMPT_COMMAND": "",
                "HISTFILE": "/dev/null",
            })
            shell = shutil.which("bash") or "/bin/bash"
            process = subprocess.Popen(
                [shell, "--noprofile", "--norc"],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                cwd=self.cwd,
                env=env,
                close_fds=True,
                start_new_session=True,
            )
        except Exception:
            os.close(master_fd)
            raise
        finally:
            os.close(slave_fd)
        os.set_blocking(master_fd, False)
        return _Session(
            session_id=str(uuid.uuid4()),
            owner=owner,
            channel_id=self.channel_id,
            process=process,
            master_fd=master_fd,
            cwd=self.cwd,
        )

    @staticmethod
    def _set_window_size(fd: int, cols: int, rows: int) -> None:
        import struct

        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 0, 0))

    def _start_command(self, session: _Session, request_id: str, data: dict) -> None:
        command = data.get("command")
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command must be non-empty text")
        command_bytes = command.encode("utf-8")
        if len(command_bytes) > MAX_COMMAND_BYTES:
            raise ValueError(f"command exceeds {MAX_COMMAND_BYTES} bytes")
        if "\x00" in command:
            raise ValueError("command may not contain NUL")
        if session.busy:
            raise RuntimeError("terminal session already has a command in flight")
        session.busy = True
        session.command_request_id = request_id
        session.command_task = asyncio.create_task(
            self._authorize_and_write(session, request_id, command)
        )

    async def _authorize_and_write(
        self, session: _Session, request_id: str, command: str
    ) -> None:
        from tools.approval import register_gateway_notify, unregister_gateway_notify

        loop = asyncio.get_running_loop()

        def notify(approval: dict) -> None:
            approval_id = str(approval.get("request_id") or "")
            session.approval_request_id = approval_id
            data = {
                "approval_id": approval_id,
                "command": str(approval.get("command") or ""),
                "description": str(approval.get("description") or ""),
                "allow_session": bool(approval.get("allow_session")),
            }
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(
                    self._emit(
                        action="approval_required",
                        request_id=request_id,
                        session_id=session.session_id,
                        data=data,
                    )
                )
            )

        register_gateway_notify(session.approval_session_key, notify)
        command_digest = hashlib.sha256(command.encode("utf-8")).hexdigest()
        try:
            decision = await asyncio.to_thread(
                self.guard_command, command, session.approval_session_key
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            decision = {"approved": False, "message": f"command guard failed: {exc}"}
        finally:
            unregister_gateway_notify(session.approval_session_key)
            session.approval_request_id = ""

        if not session.busy or session.command_request_id != request_id:
            return
        if not decision.get("approved"):
            session.busy = False
            session.command_request_id = ""
            logger.info(
                "Buzz terminal command blocked owner=%s session=%s digest=%s",
                session.owner[:12],
                session.session_id[:12],
                command_digest,
            )
            await self._emit(
                action="command_rejected",
                request_id=request_id,
                session_id=session.session_id,
                data={"message": str(decision.get("message") or "Command blocked")},
            )
            return

        try:
            await asyncio.to_thread(self._write_command, session, request_id, command)
            logger.info(
                "Buzz terminal command accepted owner=%s session=%s digest=%s",
                session.owner[:12],
                session.session_id[:12],
                command_digest,
            )
        except Exception as exc:
            session.busy = False
            session.command_request_id = ""
            await self._emit(
                action="error",
                request_id=request_id,
                session_id=session.session_id,
                data={"code": "pty_write_failed", "message": str(exc)},
            )

    def _write_command(self, session: _Session, request_id: str, command: str) -> None:
        marker = f"__BUZZ_TERMINAL_DONE_{uuid.uuid4().hex}__"
        encoded = base64.b64encode(command.encode("utf-8")).decode("ascii")
        wrapper = (
            f"__buzz_cmd=\"$(printf %s '{encoded}' | {self._base64_path} --decode)\"; "
            'eval "$__buzz_cmd"; __buzz_status=$?; '
            f'__buzz_cwd="$(printf %s "$PWD" | {self._base64_path} | tr -d "\\n")"; '
            f'printf "\\n{marker}%s:%s\\n" "$__buzz_status" "$__buzz_cwd"; '
            "unset __buzz_cmd __buzz_status __buzz_cwd\n"
        ).encode("utf-8")
        session.marker = marker.encode("ascii")
        session.command_request_id = request_id
        view = memoryview(wrapper)
        while view:
            try:
                written = os.write(session.master_fd, view)
                view = view[written:]
            except BlockingIOError:
                _, writable, _ = select.select([], [session.master_fd], [], 1.0)
                if not writable:
                    raise TimeoutError("PTY input stayed blocked")

    async def _approval_response(
        self, owner: str, session_id: str, request_id: str, data: dict
    ) -> None:
        from tools.approval import resolve_gateway_approval

        session = self._require_session(owner, session_id)
        approval_id = str(data.get("approval_id") or "")
        choice = str(data.get("choice") or "")
        if choice not in {"once", "session", "deny"}:
            raise ValueError("approval choice must be once, session, or deny")
        if not approval_id or approval_id != session.approval_request_id:
            raise PermissionError(
                "approval response does not match the pending request"
            )
        resolved = resolve_gateway_approval(
            session.approval_session_key,
            choice,
            request_id=approval_id,
        )
        if resolved != 1:
            raise RuntimeError("approval request is no longer pending")
        await self._emit(
            action="approval_received",
            request_id=request_id,
            session_id=session.session_id,
            data={"approval_id": approval_id, "choice": choice},
        )

    async def _resize(self, session: _Session, request_id: str, data: dict) -> None:
        cols = _bounded_dimension(data.get("cols"), MIN_COLS, MAX_COLS, "cols")
        rows = _bounded_dimension(data.get("rows"), MIN_ROWS, MAX_ROWS, "rows")
        self._set_window_size(session.master_fd, cols, rows)
        await self._emit(
            action="resized",
            request_id=request_id,
            session_id=session.session_id,
            data={"cols": cols, "rows": rows},
        )

    async def _ack(self, session: _Session, data: dict) -> None:
        ack_seq = data.get("seq")
        if isinstance(ack_seq, bool) or not isinstance(ack_seq, int) or ack_seq < 0:
            raise ValueError("ack seq must be a non-negative integer")
        for seq in [seq for seq in session.history if seq <= ack_seq]:
            session.history_bytes -= len(session.history.pop(seq).encode("utf-8"))

    async def _reconnect(self, session: _Session, request_id: str, data: dict) -> None:
        after_seq = data.get("after_seq", 0)
        if (
            isinstance(after_seq, bool)
            or not isinstance(after_seq, int)
            or after_seq < 0
        ):
            raise ValueError("after_seq must be a non-negative integer")
        await self._emit(
            action="reconnected",
            request_id=request_id,
            session_id=session.session_id,
            data={
                "cwd": session.cwd,
                "busy": session.busy,
                "next_seq": session.next_seq,
                "truncated": session.output_truncated,
            },
        )
        for seq, text in list(session.history.items()):
            if seq > after_seq:
                await self._emit(
                    action="output",
                    request_id=session.command_request_id or request_id,
                    session_id=session.session_id,
                    data={"seq": seq, "text": text, "replay": True},
                )

    async def _interrupt(self, session: _Session, request_id: str) -> None:
        if session.process.poll() is None:
            os.killpg(session.process.pid, signal.SIGINT)
        await self._emit(
            action="interrupted",
            request_id=request_id,
            session_id=session.session_id,
            data={},
        )

    def _on_readable(self, session_id: str) -> None:
        session = self.sessions.get(session_id)
        if session is None:
            return
        try:
            chunk = os.read(session.master_fd, 65_536)
        except BlockingIOError:
            return
        except OSError as exc:
            if exc.errno == errno.EIO:
                asyncio.create_task(
                    self._close_session(session_id, reason="shell_exited")
                )
                return
            raise
        if not chunk:
            asyncio.create_task(self._close_session(session_id, reason="shell_exited"))
            return
        session.last_activity = time.monotonic()
        session.read_buffer.extend(chunk)
        self._drain_read_buffer(session)

    def _drain_read_buffer(self, session: _Session) -> None:
        if not session.marker:
            self._queue_output(session, bytes(session.read_buffer))
            session.read_buffer.clear()
            return
        marker_index = session.read_buffer.find(session.marker)
        if marker_index < 0:
            keep = len(session.marker) + 96
            if len(session.read_buffer) > keep:
                emit_len = len(session.read_buffer) - keep
                self._queue_output(session, bytes(session.read_buffer[:emit_len]))
                del session.read_buffer[:emit_len]
            return
        line_end = session.read_buffer.find(b"\n", marker_index)
        if line_end < 0:
            return
        self._queue_output(session, bytes(session.read_buffer[:marker_index]))
        result = (
            bytes(session.read_buffer[marker_index + len(session.marker) : line_end])
            .decode("ascii", errors="replace")
            .rstrip("\r")
        )
        del session.read_buffer[: line_end + 1]
        try:
            status_raw, cwd_raw = result.split(":", 1)
            status = int(status_raw)
            cwd = base64.b64decode(cwd_raw, validate=True).decode("utf-8")
        except (ValueError, UnicodeError):
            status, cwd = 255, session.cwd
        request_id = session.command_request_id
        session.marker = b""
        session.busy = False
        session.command_request_id = ""
        session.cwd = cwd
        session.completion = (request_id, status, cwd)
        session.output_event.set()
        if session.read_buffer:
            self._queue_output(session, bytes(session.read_buffer))
            session.read_buffer.clear()

    def _queue_output(self, session: _Session, raw: bytes) -> None:
        if not raw:
            return
        text = _clean_terminal_text(session.decoder.decode(raw))
        if not text:
            return
        encoded = text.encode("utf-8")
        session.pending_output.extend(encoded)
        if len(session.pending_output) > MAX_PENDING_OUTPUT_BYTES:
            overflow = len(session.pending_output) - MAX_PENDING_OUTPUT_BYTES
            del session.pending_output[:overflow]
            session.output_truncated = True
        session.output_event.set()

    async def _output_loop(self, session: _Session) -> None:
        try:
            while True:
                await session.output_event.wait()
                session.output_event.clear()
                while session.pending_output:
                    raw = bytes(session.pending_output[:OUTPUT_CHUNK_BYTES])
                    del session.pending_output[: len(raw)]
                    text = raw.decode("utf-8", errors="replace")
                    seq = session.next_seq
                    session.next_seq += 1
                    session.history[seq] = text
                    session.history_bytes += len(raw)
                    while session.history_bytes > MAX_HISTORY_BYTES and session.history:
                        _, removed = session.history.popitem(last=False)
                        session.history_bytes -= len(removed.encode("utf-8"))
                        session.output_truncated = True
                    await self._emit(
                        action="output",
                        request_id=session.command_request_id,
                        session_id=session.session_id,
                        data={
                            "seq": seq,
                            "text": text,
                            "truncated": session.output_truncated,
                        },
                    )
                    await asyncio.sleep(OUTPUT_INTERVAL_SECS)
                if session.completion is not None:
                    request_id, status, cwd = session.completion
                    session.completion = None
                    await self._emit(
                        action="completed",
                        request_id=request_id,
                        session_id=session.session_id,
                        data={"exit_code": status, "cwd": cwd},
                    )
        except asyncio.CancelledError:
            raise

    async def _emit(
        self,
        *,
        action: str,
        request_id: str,
        session_id: str,
        data: dict,
    ) -> None:
        try:
            telemetry_request_id = _canonical_uuid(request_id, "request_id")
        except (TypeError, ValueError, AttributeError):
            # Output emitted before a command and lifecycle-driven closes have
            # no client request to correlate. Give every encrypted frame its
            # own canonical id so strict mobile consumers can still validate
            # and deduplicate it without weakening the wire contract.
            telemetry_request_id = str(uuid.uuid4())
        payload = {
            "kind": PROTOCOL_KIND,
            "version": PROTOCOL_VERSION,
            "direction": "telemetry",
            "action": action,
            "request_id": telemetry_request_id,
            "sent_at": int(time.time()),
            "channel_id": self.channel_id,
            "session_id": session_id,
            "data": data,
        }
        try:
            await self.publish_telemetry(payload)
        except Exception:
            logger.warning("Buzz terminal telemetry publish failed", exc_info=True)

    async def _close_session(
        self, session_id: str, *, reason: str, notify: bool = True
    ) -> None:
        session = self.sessions.pop(session_id, None)
        if session is None:
            return
        try:
            asyncio.get_running_loop().remove_reader(session.master_fd)
        except Exception:
            pass
        from tools.approval import unregister_gateway_notify

        unregister_gateway_notify(session.approval_session_key)
        if session.command_task and not session.command_task.done():
            session.command_task.cancel()
        if session.output_task and not session.output_task.done():
            session.output_task.cancel()
        tasks = [task for task in (session.command_task, session.output_task) if task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if session.process.poll() is None:
            try:
                os.killpg(session.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(
                    asyncio.to_thread(session.process.wait), timeout=2.0
                )
            except asyncio.TimeoutError:
                try:
                    os.killpg(session.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await asyncio.to_thread(session.process.wait)
        try:
            os.close(session.master_fd)
        except OSError:
            pass
        if notify:
            await self._emit(
                action="closed",
                request_id=session.command_request_id,
                session_id=session.session_id,
                data={"reason": reason},
            )

    async def _reaper_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(min(30.0, self.idle_ttl_secs / 2))
                now = time.monotonic()
                for session in list(self.sessions.values()):
                    reason = ""
                    if now - session.created_at >= self.absolute_ttl_secs:
                        reason = "absolute_timeout"
                    elif now - session.last_activity >= self.idle_ttl_secs:
                        reason = "idle_timeout"
                    elif session.process.poll() is not None:
                        reason = "shell_exited"
                    if reason:
                        await self._close_session(session.session_id, reason=reason)
        except asyncio.CancelledError:
            raise
