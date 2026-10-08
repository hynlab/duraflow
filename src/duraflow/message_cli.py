"""Role-based message runtime and broker-only application commands."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
from typing import Any

from .channels import ChannelRef
from .connections import broker, open_journal
from .contracts import Registry, canonical, decode, parse_json
from .messaging import Topics
from .observability import configure_logging
from .runtime import Runtime, ROLES
from .client import Client


async def service(runtime: Runtime) -> dict[str, bool]:
    configure_logging()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        async with runtime:
            await runtime.run(stop=stop, on_hard_timeout=lambda: os._exit(75))
    finally:
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    return {"stopped": True}


async def execute(args: argparse.Namespace) -> Any:
    from .cli import runtime_settings, summary

    roles = {"engine": "workflow-engine", "worker": "task-worker"}
    command = roles.get(args.command, args.command)
    database_commands = {"init", "migrate", "health", "list"}
    role = command if command in ROLES else "workflow-engine" if command in database_commands else "client"
    settings = runtime_settings(args, role=role)
    topics = Topics(settings.namespace, settings.pulsar_tenant, settings.pulsar_namespace)
    if command in ROLES:
        return await service(
            Runtime(
                role=command,
                settings=settings,
                app=args.app,
                workflows=[args.workflow.split(":v")[0]] if args.workflow else None,
            )
        )
    app = importlib.import_module(args.app) if args.app else None
    registry = getattr(app, "registry", Registry())
    if command in database_commands:
        store = open_journal(settings.database_url, settings.message_schema, settings)
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
        finally:
            await store.close()
    if not app:
        raise ValueError("--app is required for broker commands and executors")
    transport = broker(settings)
    client = None
    try:
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
        await transport.close()
