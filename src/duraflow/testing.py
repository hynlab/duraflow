"""Deterministic local integration harness; not a production broker."""

from __future__ import annotations

from typing import Any

from .legacy_client import Client, WorkflowHandle
from .contracts import ManualClock, Registry, TERMINAL, fingerprint
from .coordinator import Engine
from .runner import BroadcastBinding, Worker
from .storage import MemoryStore, Store
from .transport import MemoryTransport


class LegacyTestEnvironment:
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

    async def __aenter__(self) -> LegacyTestEnvironment:
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


class TestEnvironment:
    """The production message handlers with a retained in-memory broker and clock."""

    __test__ = False

    def __init__(
        self,
        registry: Registry,
        *,
        namespace: str = "test",
        broadcasts: tuple[BroadcastBinding, ...] = (),
        store: Any = None,
        journal: Any = None,
        clock: ManualClock | None = None,
    ):
        import asyncio
        from .broker import MemoryBroker
        from .message_store import MemoryMessageStore
        from .messaging import Topics
        from .tag_engine import TagEngine
        from .task_tags import TaskTagEngine
        from .task_worker import TaskWorker
        from .client import Client as BrokerClient
        from .workflow_engine import WorkflowEngine
        from .workflow_worker import WorkflowWorker

        self.clock = clock or ManualClock()
        self.namespace = namespace
        self.topics = Topics(namespace=namespace)
        self.store = store or MemoryMessageStore(clock=self.clock)
        self.journal = journal or MemoryMessageStore(clock=self.clock)
        self.transport = MemoryBroker(clock=self.clock)
        self.client = BrokerClient(self.transport, registry, topics=self.topics)
        self.engine = WorkflowEngine(self.store, self.transport, registry, topics=self.topics)
        self.workflows = WorkflowWorker(self.transport, registry, topics=self.topics)
        self.worker = TaskWorker(
            self.transport, registry, topics=self.topics, journal=self.journal, broadcasts=broadcasts
        )
        self.tags = TagEngine(self.store, self.transport, topics=self.topics)
        self.task_tags = TaskTagEngine(self.journal, self.transport, topics=self.topics)
        self.runtimes = (self.engine, self.workflows, self.worker, self.tags, self.task_tags)
        self.stop = asyncio.Event()
        self.running: list[Any] = []

    async def __aenter__(self) -> TestEnvironment:
        import asyncio

        for runtime in self.runtimes:
            await runtime.prepare()
        self.running = [asyncio.create_task(runtime.run(self.stop, poll_interval=0.001)) for runtime in self.runtimes]
        return self

    async def __aexit__(self, *args: Any) -> None:
        import asyncio

        self.stop.set()
        try:
            async with asyncio.timeout(3):
                await asyncio.gather(*self.running)
        except TimeoutError:
            for task in self.running:
                task.cancel()
            await asyncio.gather(*self.running, return_exceptions=True)
        for runtime in self.runtimes:
            await runtime.close()
        await self.client.close()
        await self.transport.close()
        await self.store.close()
        await self.journal.close()

    async def drain(self, *, steps: int = 100) -> None:
        import asyncio

        stable, previous = 0, None
        for _ in range(steps):
            await asyncio.sleep(0.005)
            current = (len(self.transport.publications), *(r.metrics["messages"] for r in self.runtimes))
            stable = stable + 1 if current == previous else 0
            if stable >= 3:
                return
            previous = current
        raise TimeoutError("Message harness exceeded its step limit")

    async def run(self, handle: Any, *, steps: int = 100) -> Any:
        await self.drain(steps=steps)
        return await handle.result(timeout=2)
