"""A real, separately killable engine or worker for native failure qualification."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import signal
from pathlib import Path

from duraflow import BroadcastBinding, Engine, Registry
from duraflow.executor import ProcessReplayExecutor
from duraflow.postgres import PostgresStore
from duraflow.runner import Worker
from duraflow.transport import PulsarTransport
from tests.native_fault_app import FUNCTIONS, HANDLERS, checkpoint, topic


class FaultWorker(Worker):
    async def _observe(self, context, result, error):
        await super()._observe(context, result, error)
        await checkpoint("after_observation", int(os.environ["DURAFLOW_FAULT_HANDLER"]))


async def run(role: str, index: int) -> None:
    namespace = os.environ["DURAFLOW_FAULT_NAMESPACE"]
    root = Path(os.environ["DURAFLOW_FAULT_DIRECTORY"])
    app_name = os.environ.get("DURAFLOW_FAULT_APP", "tests.native_fault_app")
    if app_name not in {"tests.native_fault_app", "tests.native_coordination_app"}:
        raise ValueError("Unknown trusted qualification application")
    registry = importlib.import_module(app_name).registry
    store = PostgresStore(
        os.environ["DURAFLOW_TEST_POSTGRES"],
        schema=os.environ["DURAFLOW_FAULT_SCHEMA"],
        pool_size=4,
        operation_timeout=5,
        statement_timeout=1,
        lock_timeout=0.5,
    )
    broker = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"], receiver_queue_size=4)
    runtime = (
        Engine(
            store,
            broker,
            registry,
            namespace=namespace,
            reconcile_interval=0.5,
            replay_executor=ProcessReplayExecutor(app_name, workers=1, timeout=3),
        )
        if role == "engine"
        else FaultWorker(
            store,
            broker,
            Registry(FUNCTIONS[index]),
            namespace=namespace,
            broadcasts=(BroadcastBinding(topic(namespace, "input"), HANDLERS[index]),),
            concurrency=1,
            lease_seconds=2,
        )
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        if role == "worker":
            await runtime.prepare()
        (root / f"{role}-{index}.ready").write_text("ready")
        await runtime.run(stop, poll_interval=0.03)
    finally:
        await runtime.close()
        await broker.close()
        await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("engine", "worker"))
    parser.add_argument("index", type=int)
    args = parser.parse_args()
    asyncio.run(run(args.role, args.index))
