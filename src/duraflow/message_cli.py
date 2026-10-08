"""Role-based message runtime and broker-only application commands."""

from __future__ import annotations

import argparse
import asyncio
import copy
import importlib
import os
import signal
from typing import Any

from .channels import ChannelRef
from .config import RuntimeSettings
from .contracts import Registry, canonical, decode, parse_json
from .executor import ProcessReplayExecutor
from .message_store import MessageStore, SQLiteMessageStore
from .messaging import Topics
from .observability import configure_logging
from .retention import RetentionPolicy
from .security import validate_production_broker, validate_production_connections
from .supervision import supervise
from .tag_engine import TagEngine
from .task_tags import TaskTagEngine
from .task_worker import TaskWorker
from .transport import PulsarTransport
from .client import Client
from .workflow_engine import WorkflowEngine
from .workflow_worker import WorkflowWorker


def open_journal(url: str, schema: str, settings: RuntimeSettings) -> MessageStore:
    if url.startswith("sqlite:///"):
        return SQLiteMessageStore(url.removeprefix("sqlite:///"))
    from .message_postgres import PostgresMessageStore

    return PostgresMessageStore(
        url,
        schema=schema,
        pool_size=settings.pool_size,
        operation_timeout=settings.operation_timeout,
        lock_timeout=settings.lock_timeout,
        statement_timeout=settings.statement_timeout,
    )


def broker(settings: RuntimeSettings) -> PulsarTransport:
    authentication = None
    if settings.pulsar_token:
        import pulsar

        authentication = pulsar.AuthenticationToken(settings.pulsar_token)
    return PulsarTransport(
        settings.broker_url,
        authentication=authentication,
        tls_trust_certs_file_path=settings.pulsar_tls_ca,
        operation_timeout=settings.operation_timeout,
        receiver_queue_size=settings.receiver_queue_size,
        max_routes=settings.max_routes,
    )


async def service(
    runtime: Any, transport: Any, store: Any, registry: Registry | None, settings: RuntimeSettings, role: str
) -> dict[str, bool]:
    await runtime.prepare()
    configure_logging()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await supervise(
            runtime, transport, store, registry, settings, stop, role=role, on_hard_timeout=lambda: os._exit(75)
        )
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    return {"stopped": True}


async def execute(args: argparse.Namespace) -> Any:
    from .cli import runtime_settings, summary

    roles = {"engine": "workflow-engine", "worker": "task-worker"}
    command = roles.get(args.command, args.command)
    database_roles = {"init", "migrate", "health", "list", "workflow-engine", "tag-engine", "task-tag-engine"}
    if command in database_roles:
        settings = runtime_settings(args)
    else:
        broker_args = copy.copy(args)
        broker_args.production = False
        broker_args.database = None
        environment = {
            key: value
            for key, value in os.environ.items()
            if key not in {"DURAFLOW_DATABASE_URL", "DURAFLOW_DATABASE_URL_FILE"}
        }
        settings = runtime_settings(broker_args, environment=environment)
        mode = os.environ.get("DURAFLOW_PRODUCTION", "false").lower()
        if mode not in {"true", "false", "1", "0"}:
            raise ValueError("Invalid production mode")
        production = args.production if args.production is not None else mode in {"true", "1"}
        if production:
            validate_production_broker(settings.broker_url, settings.pulsar_token, settings.pulsar_tls_ca)
            if command == "task-worker":
                if not args.journal:
                    raise ValueError("Production task workers require an independent journal URL")
                validate_production_connections(
                    args.journal, settings.broker_url, settings.pulsar_token, settings.pulsar_tls_ca
                )
    topics = Topics(settings.namespace, args.tenant, args.environment)
    app = importlib.import_module(args.app) if args.app else None
    registry = getattr(app, "registry", Registry())
    runtime: Any
    if command in database_roles:
        store = open_journal(settings.database_url, args.schema, settings)
        try:
            if command in {"init", "migrate"}:
                initialize = getattr(store, "initialize", None)
                if initialize:
                    await initialize()
                return {"initialized": True, "execution_protocol": 2, "schema_version": 1}
            if command == "health":
                return {"store_readable": await store.ping(), "namespace": settings.namespace}
            if command == "list":
                if not 1 <= args.limit <= 1000:
                    raise ValueError("List limit must be in 1..1000")
                prefix = canonical([settings.namespace])[:-1] + ","
                rows = await store.list_states(prefix, after=args.after, limit=args.limit)
                return [
                    summary(row["runs"][row["current"]])
                    for row in rows
                    if "runs" in row and set(args.tag) <= set(row["runs"][row["current"]]["tags"])
                ]
            if not app and command == "workflow-engine" and not args.workflow:
                raise ValueError("Use --workflow NAME or --app to configure a workflow engine")
            transport = broker(settings)
            if command == "workflow-engine":
                workflows = (
                    registry if registry.workflows else [ref.name for ref in getattr(app, "workflows", {}).values()]
                )
                if args.workflow:
                    workflows = [args.workflow.split(":v")[0]]
                runtime = WorkflowEngine(
                    store,
                    transport,
                    workflows,
                    topics=topics,
                    concurrency=settings.concurrency,
                    max_commands=settings.max_commands,
                    retention_policy=RetentionPolicy(settings.retention_seconds, settings.redelivery_safety_horizon)
                    if settings.production
                    else None,
                )
            elif command == "tag-engine":
                runtime = TagEngine(store, transport, topics=topics)
            else:
                runtime = TaskTagEngine(store, transport, topics=topics)
            try:
                return await service(
                    runtime, transport, store, registry if registry.workflows else None, settings, command
                )
            finally:
                await transport.close()
        finally:
            await store.close()
    if not app:
        raise ValueError("--app is required for broker commands and executors")
    transport = broker(settings)
    journal = None
    client = None
    try:
        if command == "workflow-worker":
            runtime = WorkflowWorker(
                transport,
                registry,
                topics=topics,
                replay_executor=ProcessReplayExecutor(
                    args.app, workers=settings.replay_workers, timeout=settings.replay_timeout
                ),
                concurrency=settings.replay_workers,
            )
            return await service(runtime, transport, None, registry, settings, command)
        if command == "task-worker":
            journal_url = args.journal or "sqlite:///duraflow-tasks.db"
            journal = open_journal(journal_url, args.schema + "_tasks", settings)
            runtime = TaskWorker(
                transport,
                registry,
                topics=topics,
                journal=journal,
                broadcasts=getattr(app, "broadcasts", ()),
                concurrency=settings.concurrency,
                lease_seconds=settings.lease_seconds,
            )
            return await service(runtime, transport, journal, registry, settings, command)
        client = Client(transport, registry, topics=topics)
        contracts = getattr(app, "workflows", {})
        if command == "start":
            ref = contracts.get(args.workflow) or registry.resolve(args.workflow).ref
            handle = await client.start(
                ref,
                decode(parse_json(args.input), ref.input_type),
                request_id=args.request_id,
                workflow_id=args.workflow_id,
            )
            return {"run_id": handle.run_id, "workflow_id": handle.workflow_id}
        selected = args.workflow
        if selected is None and len(registry.workflows) == 1:
            selected = next(iter(registry.workflows))
        if selected is None and len(contracts) == 1:
            selected = next(iter(contracts))
        if selected is None:
            raise ValueError("Use --workflow name:vN to identify the workflow contract")
        handle = client.get_handle(contracts.get(selected) or selected, args.run_id)
        if command == "describe":
            state = await handle.describe()
            return state if args.include_payload else summary(state)
        if command == "history":
            return await handle.history(after=args.after, limit=args.limit)
        if command == "attempts":
            return {
                key: node["attempts"] for key, node in (await handle.describe())["nodes"].items() if "attempts" in node
            }
        if command == "result":
            return await handle.result(timeout=args.timeout)
        if command == "signal":
            ref = getattr(app, "signals", {}).get(args.channel)
            if ref is None:
                raise ValueError("Declare channels in the application's signals mapping")
            return await handle.signal(
                ChannelRef(ref.name, ref.payload_type),
                decode(parse_json(args.input), ref.payload_type),
                signal_id=args.signal_id,
            )
        if command in {"cancel", "terminate", "resume", "retry"}:
            await handle._control(
                command,
                actor=args.actor,
                reason=args.reason,
                request_id=args.request_id,
                node_id=args.node_id if command == "retry" else None,
            )
            return {"accepted": True, "workflow_id": args.run_id}
        if command == "archive":
            await handle.archive(
                actor=args.actor, reason=args.reason, retention=args.retention, safety_horizon=args.safety_horizon
            )
            return {"accepted": True, "workflow_id": args.run_id}
        raise ValueError("This command applies to protocol 1; use --legacy for existing runs")
    finally:
        if client:
            await client.close()
        if journal:
            await journal.close()
        await transport.close()
