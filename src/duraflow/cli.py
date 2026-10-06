"""Trusted operator CLI; module imports are explicit configuration, never messages."""
from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import signal
import sys
from typing import Any

from .client import Client
from .contracts import DuraflowError, Registry, decode, parse_json
from .coordinator import Engine
from .runner import Worker
from .storage import SQLiteStore, Store
from .transport import PulsarTransport


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="duraflow")
    root.add_argument("--database", default=os.environ.get("DURAFLOW_DATABASE_URL", "sqlite:///duraflow.db"))
    root.add_argument("--namespace", default="default")
    root.add_argument("--app", help="Trusted module exporting registry, optional broadcasts and signals")
    root.add_argument("--broker", default=os.environ.get("DURAFLOW_PULSAR_URL", "pulsar://localhost:6650"))
    commands = root.add_subparsers(dest="command", required=True)
    for command in ("init", "health", "engine", "worker"):
        commands.add_parser(command)
    listing = commands.add_parser("list")
    listing.add_argument("--after", default="")
    listing.add_argument("--limit", type=int, default=100)
    listing.add_argument("--tag", action="append", default=[])
    start = commands.add_parser("start")
    start.add_argument("workflow", help="Registered name:vN")
    start.add_argument("--input", required=True, help="JSON input")
    start.add_argument("--request-id", required=True)
    start.add_argument("--workflow-id")
    for command in ("describe", "history", "attempts", "signal", "cancel", "terminate", "resume", "retry", "archive"):
        sub = commands.add_parser(command)
        sub.add_argument("run_id")
        if command == "describe":
            sub.add_argument("--include-payload", action="store_true")
        if command == "history":
            sub.add_argument("--after", type=int, default=0)
            sub.add_argument("--limit", type=int, default=100)
        if command == "signal":
            sub.add_argument("channel")
            sub.add_argument("--input", required=True)
            sub.add_argument("--signal-id", required=True)
        if command in {"cancel", "terminate", "resume", "retry", "archive"}:
            sub.add_argument("--actor", required=True)
            sub.add_argument("--reason", required=True)
            sub.add_argument("--yes", action="store_true", required=True)
            if command != "archive":
                sub.add_argument("--request-id", required=True)
        if command == "retry":
            sub.add_argument("node_id")
        if command == "archive":
            sub.add_argument("--retention", type=float, required=True)
            sub.add_argument("--safety-horizon", type=float, required=True)
    return root


async def open_store(url: str) -> Store:
    if url.startswith("sqlite:///"):
        return SQLiteStore(url.removeprefix("sqlite:///"))
    from .postgres import PostgresStore
    return PostgresStore(url)


def summary(state: dict[str, Any]) -> dict[str, Any]:
    fields = ("namespace", "run_id", "workflow_id", "status", "revision", "manifest", "tags",
              "created_at", "finished_at", "blocked_reason", "error", "continued_run_id", "archived")
    return {key: state[key] for key in fields}


async def service(args: argparse.Namespace, store: Store, app: Any, registry: Registry) -> dict[str, bool]:
    if app is None:
        raise ValueError("--app is required for engine/worker")
    authentication = None
    token = os.environ.get("DURAFLOW_PULSAR_TOKEN")
    if token:
        import pulsar
        authentication = pulsar.AuthenticationToken(token)
    transport = PulsarTransport(args.broker, authentication=authentication,
                                tls_trust_certs_file_path=os.environ.get("DURAFLOW_PULSAR_TLS_CA"))
    runtime = (Engine(store, transport, registry, namespace=args.namespace) if args.command == "engine" else
               Worker(store, transport, registry, namespace=args.namespace, broadcasts=getattr(app, "broadcasts", ())))
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    running, requested = asyncio.create_task(runtime.run(stop)), asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait({running, requested}, return_when=asyncio.FIRST_COMPLETED)
        if running in done:
            await running
        else:
            try:
                await asyncio.wait_for(asyncio.shield(running), timeout=30)
            except TimeoutError:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
    finally:
        requested.cancel()
        if not running.done():
            running.cancel()
        await asyncio.gather(running, requested, return_exceptions=True)
        if isinstance(runtime, Worker):
            await runtime.close()
        await transport.close()
    return {"stopped": True}


async def execute(args: argparse.Namespace) -> Any:
    app = importlib.import_module(args.app) if args.app else None
    registry = getattr(app, "registry", Registry())
    store = await open_store(args.database)
    client = Client(store, registry, namespace=args.namespace)
    try:
        if args.command == "init":
            initialize = getattr(store, "initialize", None)
            if initialize is not None:
                await initialize()
            return {"schema_version": 1, "initialized": True}
        if args.command == "health":
            await store.scan(args.namespace, limit=1)
            return {"store_readable": True, "namespace": args.namespace}
        if args.command in {"engine", "worker"}:
            return await service(args, store, app, registry)
        if args.command == "list":
            return [summary(state) for state in await client.list(after=args.after, limit=args.limit, tags=tuple(args.tag))]
        if args.command == "start":
            definition = registry.resolve(args.workflow)
            value = decode(parse_json(args.input), definition.ref.input_type)
            handle = await client.start(definition.ref, value, request_id=args.request_id, workflow_id=args.workflow_id)
            return {"run_id": handle.run_id}
        handle = client.get_handle(args.run_id)
        if args.command == "describe":
            state = await handle.describe()
            return state if args.include_payload else summary(state)
        if args.command == "history":
            return await handle.history(after=args.after, limit=args.limit)
        if args.command == "attempts":
            return {key: node["attempts"] for key, node in (await handle.describe())["nodes"].items() if "attempts" in node}
        if args.command == "signal":
            ref = getattr(app, "signals", {}).get(args.channel)
            if ref is None:
                raise ValueError("Declare the typed channel in your --app signals mapping")
            await handle.signal(ref, decode(parse_json(args.input), ref.payload_type), signal_id=args.signal_id)
        elif args.command == "archive":
            await handle.archive(actor=args.actor, reason=args.reason, retention=args.retention,
                                 safety_horizon=args.safety_horizon)
        else:
            await handle._control(
                args.command, actor=args.actor, reason=args.reason, request_id=args.request_id,
                node_id=args.node_id if args.command == "retry" else None,
            )
        return {"accepted": True, "run_id": handle.run_id}
    finally:
        await store.close()


def main() -> None:
    try:
        print(json.dumps(asyncio.run(execute(parser().parse_args())), indent=2, ensure_ascii=False))
    except (DuraflowError, ValueError) as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(2) from None


if __name__ == "__main__":
    main()
