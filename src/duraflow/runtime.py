"""Code-configured, role-based runtime with explicit connection ownership."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import AsyncExitStack
from typing import Any, Callable

from .client import Client
from .config import RuntimeSettings, RUNTIME_ROLES
from .connections import broker, open_journal
from .contracts import BroadcastBinding, Registry
from .executor import ProcessReplayExecutor
from .message_runtime import Consumer
from .message_store import MessageStore
from .messaging import Topics
from .retention import RetentionPolicy
from .supervision import supervise
from .tag_engine import TagEngine
from .task_tags import TaskTagEngine
from .task_worker import TaskWorker
from .transport import Transport
from .workflow_engine import WorkflowEngine
from .workflow_worker import WorkflowWorker

ROLES = RUNTIME_ROLES
DATABASE_ROLES = frozenset({"workflow-engine", "task-worker", "tag-engine", "task-tag-engine"})


class Runtime:
    """Own one role's connections. Enter the context, then await run(stop=event).

    Construction never reads the environment or connects. Use
    RuntimeSettings.from_environment(role=...) to opt into environment loading.
    A runtime is single-use; create a new instance when restarting a role.
    """

    def __init__(
        self,
        *,
        role: str,
        settings: RuntimeSettings | None = None,
        registry: Registry | None = None,
        app: str | None = None,
        workflows: list[str] | None = None,
        broadcasts: tuple[BroadcastBinding, ...] | None = None,
    ):
        if role not in ROLES:
            raise ValueError("Unknown runtime role")
        self.role, self.settings = role, settings or RuntimeSettings()
        self.app = app
        module = importlib.import_module(app) if app else None
        self.registry = registry if registry is not None else getattr(module, "registry", Registry())
        self.broadcasts = broadcasts if broadcasts is not None else getattr(module, "broadcasts", ())
        self.workflows = workflows
        if role == "workflow-engine" and workflows is None and not self.registry.workflows:
            self.workflows = [ref.name for ref in getattr(module, "workflows", {}).values()]
        if role == "workflow-engine" and not (self.workflows or self.registry.workflows):
            raise ValueError("Workflow engines require a registry or workflow names")
        if role == "workflow-worker" and (not app or not self.registry.workflows):
            raise ValueError("Workflow workers require an importable app module exporting a workflow registry")
        if role == "task-worker" and not self.registry.tasks:
            raise ValueError("Task workers require a task registry")
        if role == "task-worker" and self.settings.production and not self.settings.task_journal_url:
            raise ValueError("Production task workers require an independent journal URL")
        self.topics = Topics(self.settings.namespace, self.settings.pulsar_tenant, self.settings.pulsar_namespace)
        self.store: MessageStore | None = None
        self.transport: Transport | None = None
        self.consumer: Consumer | None = None
        self._client: Client | None = None
        self._stack = AsyncExitStack()
        self._entered = False
        self._used = False
        self._running = False

    def _open_store(self) -> MessageStore:
        settings = self.settings
        if self.role == "task-worker":
            return open_journal(
                settings.task_journal_url or "sqlite:///duraflow-tasks.db",
                settings.message_schema + "_tasks",
                settings,
            )
        return open_journal(settings.database_url, settings.message_schema, settings)

    async def initialize(self) -> None:
        """Initialize this role's journal without connecting to Pulsar; safe to repeat."""
        if self._used:
            raise RuntimeError("Initialize before entering the runtime")
        if self.role not in DATABASE_ROLES:
            return
        store = self._open_store()
        try:
            initialize = getattr(store, "initialize", None)
            if initialize is not None:
                await initialize()
        finally:
            await store.close()

    async def __aenter__(self) -> Runtime:
        if self._used:
            raise RuntimeError("Runtime instances are single-use")
        self._used = True
        try:
            if self.role in DATABASE_ROLES:
                self.store = self._open_store()
                self._stack.push_async_callback(self.store.close)
            self.transport = broker(self.settings)
            self._stack.push_async_callback(self.transport.close)
            if self.role == "client":
                self._client = Client(self.transport, self.registry, topics=self.topics)
                self._stack.push_async_callback(self._client.close)
            else:
                self.consumer = self._consumer()
                self._stack.push_async_callback(self.consumer.close)
                await self.consumer.prepare()
        except BaseException:
            await self._stack.aclose()
            raise
        self._entered = True
        return self

    def _consumer(self) -> Consumer:
        assert self.transport is not None
        settings = self.settings
        if self.role == "workflow-worker":
            assert self.app is not None
            return WorkflowWorker(
                self.transport,
                self.registry,
                topics=self.topics,
                replay_executor=ProcessReplayExecutor(
                    self.app, workers=settings.replay_workers, timeout=settings.replay_timeout
                ),
                concurrency=settings.replay_workers,
            )
        assert self.store is not None
        if self.role == "workflow-engine":
            return WorkflowEngine(
                self.store,
                self.transport,
                self.workflows if self.workflows is not None else self.registry,
                topics=self.topics,
                concurrency=settings.concurrency,
                max_commands=settings.max_commands,
                retention_policy=RetentionPolicy(settings.retention_seconds, settings.redelivery_safety_horizon)
                if settings.production
                else None,
            )
        if self.role == "task-worker":
            return TaskWorker(
                self.transport,
                self.registry,
                topics=self.topics,
                journal=self.store,
                broadcasts=self.broadcasts,
                concurrency=settings.concurrency,
                lease_seconds=settings.lease_seconds,
            )
        if self.role == "tag-engine":
            return TagEngine(self.store, self.transport, topics=self.topics)
        return TaskTagEngine(self.store, self.transport, topics=self.topics)

    @property
    def client(self) -> Client:
        if not self._entered or self._client is None:
            raise RuntimeError("Enter a client runtime before accessing its client")
        return self._client

    async def run(
        self, *, stop: asyncio.Event | None = None, on_hard_timeout: Callable[[], None] | None = None
    ) -> None:
        """Serve until stopped or cancelled; installs no process signal handlers."""
        if not self._entered or self.consumer is None or self._running:
            raise RuntimeError("Enter a service runtime and run it once at a time")
        self._running = True
        try:
            await supervise(
                self.consumer,
                self.transport,
                self.store,
                None if self.role == "workflow-engine" and not self.registry.workflows else self.registry,
                self.settings,
                stop if stop is not None else asyncio.Event(),
                role=self.role,
                on_hard_timeout=on_hard_timeout,
                close_resources=False,
            )
        finally:
            self._running = False

    async def __aexit__(self, *args: Any) -> None:
        self._entered = False
        await self._stack.aclose()
