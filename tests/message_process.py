"""Separately killable protocol-2 runtime roles for fresh-process recovery tests."""

import argparse
import asyncio
import os
import signal
from pathlib import Path

from duraflow import Topics, WorkflowEngine, WorkflowWorker, TaskWorker
from duraflow.executor import ProcessReplayExecutor
from duraflow.message_postgres import PostgresMessageStore
from duraflow.transport import PulsarTransport
from tests.message_app import registry


class PausingTaskWorker(TaskWorker):
    async def handle(self, message, delivery):
        await super().handle(message, delivery)
        if (
            message.kind == "execute_task"
            and message.body["node_id"] == "1.0"
            and os.environ.get("DURAFLOW_MESSAGE_FAULT") == "after_observation"
        ):
            (Path(os.environ["DURAFLOW_MESSAGE_FIXTURE"]) / "after_observation.ready").write_text("ready")
            while True:
                await asyncio.sleep(0.02)


async def main(role, index):
    namespace = os.environ["DURAFLOW_MESSAGE_NAMESPACE"]
    topics = Topics(namespace=namespace)
    transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
    store = None
    if role == "engine":
        store = PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace)
        runtime = WorkflowEngine(store, transport, ["message-order"], topics=topics)
    elif role == "workflow":
        runtime = WorkflowWorker(
            transport, registry, topics=topics, replay_executor=ProcessReplayExecutor("tests.message_app")
        )
    else:
        store = PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace + "_tasks")
        runtime = PausingTaskWorker(transport, registry, topics=topics, journal=store, lease_seconds=2)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    try:
        await runtime.prepare()
        (Path(os.environ["DURAFLOW_MESSAGE_FIXTURE"]) / f"{role}-{index}.ready").write_text("ready")
        await runtime.run(stop, poll_interval=0.01)
    finally:
        await runtime.close()
        await transport.close()
        if store:
            await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("engine", "workflow", "task"))
    parser.add_argument("index", type=int)
    args = parser.parse_args()
    asyncio.run(main(args.role, args.index))
