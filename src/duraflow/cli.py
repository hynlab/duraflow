"""Trusted operator CLI; module imports are explicit configuration, never messages."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import signal
import sys
from pathlib import Path
from typing import Any

from .client import Client
from .config import RuntimeSettings
from .observability import configure_logging
from .quarantine import decode_quarantine, replay_quarantine, summary as quarantine_summary
from .supervision import supervise
from .contracts import DuraflowError, Registry, decode, parse_json
from .coordinator import Engine
from .executor import ProcessReplayExecutor
from .runner import Worker
from .storage import SQLiteStore, Store
from .transport import PulsarTransport


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="duraflow")
    root.add_argument("--database", default=os.environ.get("DURAFLOW_DATABASE_URL", "sqlite:///duraflow.db"))
    root.add_argument("--namespace", default=os.environ.get("DURAFLOW_NAMESPACE", "default"))
    root.add_argument("--app", help="Trusted module exporting registry, optional broadcasts and signals")
    root.add_argument("--broker", default=os.environ.get("DURAFLOW_PULSAR_URL", "pulsar://localhost:6650"))
    root.add_argument("--production", action="store_true", default=None)
    root.add_argument("--concurrency", type=int)
    root.add_argument("--lease-seconds", type=float)
    root.add_argument("--shutdown-timeout", type=float)
    root.add_argument("--probe-port", type=int)
    root.add_argument("--probe-host")
    commands = root.add_subparsers(dest="command", required=True)
    for command in ("init", "migrate", "health", "engine", "worker"):
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
    peek = commands.add_parser("dlq-peek")
    peek.add_argument("--topic", required=True)
    peek.add_argument("--limit", type=int, default=10)
    peek.add_argument("--include-payload", action="store_true")
    replay = commands.add_parser("dlq-replay")
    replay.add_argument("--file", required=True)
    replay.add_argument("--actor", required=True)
    replay.add_argument("--reason", required=True)
    replay.add_argument("--request-id", required=True)
    replay.add_argument("--yes", action="store_true", required=True)
    return root


async def open_store(url: str, settings: RuntimeSettings | None = None) -> Store:
    settings = settings or RuntimeSettings(database_url=url)
    if url.startswith("sqlite:///"):
        return SQLiteStore(url.removeprefix("sqlite:///"))
    from .postgres import PostgresStore

    return PostgresStore(
        url,
        pool_size=settings.pool_size,
        operation_timeout=settings.operation_timeout,
        lock_timeout=settings.lock_timeout,
        statement_timeout=settings.statement_timeout,
    )


def summary(state: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "namespace",
        "run_id",
        "workflow_id",
        "status",
        "revision",
        "manifest",
        "tags",
        "created_at",
        "finished_at",
        "blocked_reason",
        "error",
        "continued_run_id",
        "archived",
    )
    return {key: state[key] for key in fields}


async def service(args: argparse.Namespace, store: Store, app: Any, registry: Registry) -> dict[str, bool]:
    if app is None:
        raise ValueError("--app is required for engine/worker")
    settings = runtime_settings(args)
    configure_logging()
    authentication = None
    token = os.environ.get("DURAFLOW_PULSAR_TOKEN")
    if token:
        import pulsar

        authentication = pulsar.AuthenticationToken(token)
    transport = PulsarTransport(
        settings.broker_url,
        authentication=authentication,
        tls_trust_certs_file_path=os.environ.get("DURAFLOW_PULSAR_TLS_CA"),
        receiver_queue_size=settings.receiver_queue_size,
        max_routes=settings.max_routes,
    )
    if args.command == "engine":
        runtime: Any = Engine(
            store,
            transport,
            registry,
            namespace=settings.namespace,
            batch_size=settings.batch_size,
            max_commands=settings.max_commands,
            replay_executor=ProcessReplayExecutor(
                args.app, workers=settings.replay_workers, timeout=settings.replay_timeout
            ),
        )
    else:
        runtime = Worker(
            store,
            transport,
            registry,
            namespace=settings.namespace,
            broadcasts=getattr(app, "broadcasts", ()),
            concurrency=settings.concurrency,
            lease_seconds=settings.lease_seconds,
        )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, stop.set)

    def hard_exit() -> None:
        # Only this dedicated CLI process may exit forcibly. Library APIs never do.
        # Lease fencing and business idempotency still govern recovery elsewhere.
        os._exit(75)

    try:
        await supervise(
            runtime, transport, store, registry, settings, stop, role=args.command, on_hard_timeout=hard_exit
        )
    finally:
        for sig in signals:
            loop.remove_signal_handler(sig)
    return {"stopped": True}


async def execute(args: argparse.Namespace) -> Any:
    app = importlib.import_module(args.app) if args.app else None
    registry = getattr(app, "registry", Registry())
    settings = runtime_settings(args)
    store = await open_store(settings.database_url, settings)
    client = Client(store, registry, namespace=args.namespace)
    try:
        if args.command in {"init", "migrate"}:
            initialize = getattr(store, "initialize", None)
            if initialize is not None:
                await initialize()
            version = getattr(store, "schema_version", None)
            return {"schema_version": await version() if version is not None else 1, "initialized": True}
        if args.command == "health":
            await store.scan(args.namespace, limit=1)
            return {"store_readable": True, "namespace": args.namespace}
        if args.command in {"engine", "worker"}:
            return await service(args, store, app, registry)
        if args.command == "list":
            return [
                summary(state) for state in await client.list(after=args.after, limit=args.limit, tags=tuple(args.tag))
            ]
        if args.command == "start":
            definition = registry.resolve(args.workflow)
            value = decode(parse_json(args.input), definition.ref.input_type)
            handle = await client.start(definition.ref, value, request_id=args.request_id, workflow_id=args.workflow_id)
            return {"run_id": handle.run_id}
        if args.command == "dlq-replay":
            return {
                "requeued": await replay_quarantine(
                    client,
                    Path(args.file).read_bytes(),
                    actor=args.actor,
                    reason=args.reason,
                    request_id=args.request_id,
                )
            }
        if args.command == "dlq-peek":
            if not 1 <= args.limit <= 100:
                raise ValueError("DLQ inspection limit must be 1..100")
            authentication = None
            token = os.environ.get("DURAFLOW_PULSAR_TOKEN")
            if token:
                import pulsar

                authentication = pulsar.AuthenticationToken(token)
            broker = PulsarTransport(
                settings.broker_url,
                authentication=authentication,
                tls_trust_certs_file_path=os.environ.get("DURAFLOW_PULSAR_TLS_CA"),
            )
            result, seen, receipts = [], set(), []
            try:
                for _ in range(args.limit):
                    delivery = await broker.receive(args.topic, "duraflow-operator-inspection")
                    if delivery is None:
                        break
                    receipts.append(delivery)
                    entry = decode_quarantine(delivery.data)
                    if entry["quarantine_id"] not in seen:
                        result.append(entry if args.include_payload else quarantine_summary(entry))
                        seen.add(entry["quarantine_id"])
                return result
            finally:
                for delivery in receipts:
                    await broker.nack(delivery)
                await broker.close()
        handle = client.get_handle(args.run_id)
        if args.command == "describe":
            state = await handle.describe()
            return state if args.include_payload else summary(state)
        if args.command == "history":
            return await handle.history(after=args.after, limit=args.limit)
        if args.command == "attempts":
            return {
                key: node["attempts"] for key, node in (await handle.describe())["nodes"].items() if "attempts" in node
            }
        if args.command == "signal":
            ref = getattr(app, "signals", {}).get(args.channel)
            if ref is None:
                raise ValueError("Declare the typed channel in your --app signals mapping")
            await handle.signal(ref, decode(parse_json(args.input), ref.payload_type), signal_id=args.signal_id)
        elif args.command == "archive":
            await handle.archive(
                actor=args.actor, reason=args.reason, retention=args.retention, safety_horizon=args.safety_horizon
            )
        else:
            await handle._control(
                args.command,
                actor=args.actor,
                reason=args.reason,
                request_id=args.request_id,
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


def runtime_settings(args: argparse.Namespace) -> RuntimeSettings:
    overrides = {"database_url": args.database, "broker_url": args.broker, "namespace": args.namespace}
    for field in ("production", "concurrency", "lease_seconds", "shutdown_timeout", "probe_port", "probe_host"):
        value = getattr(args, field, None)
        if value is not None:
            overrides[field] = value
    return RuntimeSettings.from_environment(os.environ, **overrides)


if __name__ == "__main__":
    main()
