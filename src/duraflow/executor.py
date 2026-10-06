"""Bounded replay fault containment. This is not a hostile-code sandbox."""
from __future__ import annotations

import asyncio
import sys
from typing import Protocol

from .contracts import (
    NonDeterminism, ProtocolError, UnsupportedWorkflow, WorkflowBlocked,
    WorkflowDefinition, canonical, duration, parse_json,
)
from .replay import Activation, replay
from .storage import State


class ReplayExecutor(Protocol):
    async def execute(self, definition: WorkflowDefinition, state: State) -> Activation: ...
    async def close(self) -> None: ...


class InlineReplayExecutor:
    """Deterministic tests/development only; it has no wall-time watchdog."""

    async def execute(self, definition: WorkflowDefinition, state: State) -> Activation:
        return replay(definition, state)

    async def close(self) -> None:
        pass


class ProcessReplayExecutor:
    """Reusable, bounded subprocesses bootstrapped from an explicitly trusted app.

    Only JSON crosses the pipe. Registry/code selection comes from configuration,
    never message-provided module paths, pickle, eval, or serialized functions.
    A watchdog kills a stuck activation including stuck coroutine finalizers.
    """

    def __init__(self, app: str, *, workers: int = 2, timeout: float = 5.0,
                 startup_timeout: float = 15.0, max_bytes: int = 16 * 1024 * 1024):
        if not app or any(not part.isidentifier() for part in app.split(".")):
            raise ValueError("An importable trusted application module is required")
        if not 1 <= workers <= 32 or not 1024 <= max_bytes <= 64 * 1024 * 1024:
            raise ValueError("Invalid replay pool or message bound")
        duration(timeout)
        duration(startup_timeout)
        self.app, self.timeout, self.startup_timeout = app, timeout, startup_timeout
        self.max_bytes = max_bytes
        self._semaphore = asyncio.Semaphore(workers)
        self._idle: list[asyncio.subprocess.Process] = []
        self._processes: set[asyncio.subprocess.Process] = set()
        self._closed = False

    async def _discard(self, process: asyncio.subprocess.Process) -> None:
        self._processes.discard(process)
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
        await process.wait()
        if process.stdin is not None:
            process.stdin.close()

    async def _start(self) -> asyncio.subprocess.Process:
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "duraflow.replay_child", "--app", self.app,
            "--max-bytes", str(self.max_bytes),
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, limit=self.max_bytes + 1,
        )
        self._processes.add(process)
        try:
            assert process.stdout is not None
            hello = await asyncio.wait_for(process.stdout.readline(), self.startup_timeout)
            if parse_json(hello) != {"ready": True}:
                raise WorkflowBlocked("REPLAY_BOOTSTRAP_FAILED")
            return process
        except BaseException:
            await self._discard(process)
            raise

    async def execute(self, definition: WorkflowDefinition, state: State) -> Activation:
        if self._closed:
            raise WorkflowBlocked("REPLAY_EXECUTOR_CLOSED")
        if definition.manifest != state["manifest"]:
            raise NonDeterminism("Pinned workflow identity does not match")
        request = canonical({"state": state}).encode() + b"\n"
        if len(request) > self.max_bytes:
            raise WorkflowBlocked("REPLAY_INPUT_LIMIT_EXCEEDED")
        async with self._semaphore:
            if self._closed:
                raise WorkflowBlocked("REPLAY_EXECUTOR_CLOSED")
            process: asyncio.subprocess.Process | None = None
            try:
                process = self._idle.pop() if self._idle else await self._start()

                async def exchange() -> bytes:
                    assert process is not None and process.stdin is not None and process.stdout is not None
                    process.stdin.write(request)
                    await process.stdin.drain()
                    return await process.stdout.readline()

                raw = await asyncio.wait_for(exchange(), self.timeout)
                if not raw or len(raw) > self.max_bytes:
                    raise WorkflowBlocked("REPLAY_EXECUTOR_INVALID_RESPONSE")
                response = parse_json(raw)
                if not isinstance(response, dict) or type(response.get("ok")) is not bool:
                    raise WorkflowBlocked("REPLAY_EXECUTOR_INVALID_RESPONSE")
                self._idle.append(process)
                process = None
                if not response["ok"]:
                    code = response.get("code")
                    if code == "NonDeterminism":
                        raise NonDeterminism("Isolated replay diverged from committed history")
                    if code == "UnsupportedWorkflow":
                        raise UnsupportedWorkflow("Isolated replay attempted an unsupported operation")
                    raise WorkflowBlocked("REPLAY_EXECUTOR_FAILED")
                if response.get("kind") not in {"schedule", "waiting", "completed", "failed"}:
                    raise WorkflowBlocked("REPLAY_EXECUTOR_INVALID_RESPONSE")
                return Activation(response["kind"], response.get("value"))
            except TimeoutError as exc:
                raise WorkflowBlocked("REPLAY_DEADLINE_EXCEEDED") from exc
            except (OSError, ValueError, ProtocolError) as exc:
                if isinstance(exc, (NonDeterminism, UnsupportedWorkflow)):
                    raise
                raise WorkflowBlocked("REPLAY_EXECUTOR_FAILED") from exc
            finally:
                if process is not None:
                    await self._discard(process)

    async def close(self) -> None:
        self._closed = True
        self._idle.clear()
        await asyncio.gather(*(self._discard(p) for p in list(self._processes)))

    async def __aenter__(self) -> ProcessReplayExecutor:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
