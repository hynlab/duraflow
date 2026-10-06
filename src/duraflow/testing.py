"""Deterministic local integration harness; not a production broker."""

from __future__ import annotations

from typing import Any

from .client import Client, WorkflowHandle
from .contracts import ManualClock, Registry, TERMINAL, fingerprint
from .coordinator import Engine
from .runner import BroadcastBinding, Worker
from .storage import MemoryStore, Store
from .transport import MemoryTransport


class TestEnvironment:
    __test__ = False

    def __init__(
        self,
        registry: Registry,
        *,
        broadcasts: tuple[BroadcastBinding, ...] = (),
        store: Store | None = None,
        namespace: str = "test",
    ):
        self.clock = ManualClock()
        self.store = store or MemoryStore()
        self.transport = MemoryTransport()
        self.client = Client(self.store, registry, namespace=namespace, clock=self.clock)
        self.engine = Engine(self.store, self.transport, registry, namespace=namespace, clock=self.clock)
        self.worker = Worker(
            self.store, self.transport, registry, namespace=namespace, clock=self.clock, broadcasts=broadcasts
        )
        self.namespace = namespace

    async def __aenter__(self) -> TestEnvironment:
        await self.worker.prepare()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.worker.close()
        await self.transport.close()
        await self.store.close()

    async def drain(self, *, steps: int = 100) -> None:
        previous = None
        for _ in range(steps):
            await self.engine.tick()
            worked = False
            for _ in range(max(1, len(self.worker.bindings))):
                worked = await self.worker.step() or worked
            await self.engine.tick()
            current = fingerprint(await self.store.scan(self.namespace, limit=10000))
            if current == previous and not worked:
                return
            previous = current
        raise TimeoutError("Harness exceeded step limit; possible infinite workflow")

    async def run(self, handle: WorkflowHandle, *, steps: int = 100) -> Any:
        await self.drain(steps=steps)
        state = await handle.describe()
        if state["status"] in TERMINAL | {"BLOCKED"}:
            return await handle.result(timeout=1)
        raise TimeoutError("Workflow is waiting; advance the clock or send its signal")
