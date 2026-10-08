"""Separately killable protocol-2 runtime roles for fresh-process recovery tests."""

import argparse
import asyncio
import os
import signal
from pathlib import Path

from duraflow import Topics, WorkflowEngine, WorkflowWorker, TaskWorker, TagEngine, TaskTagEngine
from duraflow.executor import ProcessReplayExecutor
from duraflow.message_postgres import PostgresMessageStore
from duraflow.transport import PulsarTransport
from tests.message_app import registry


async def checkpoint(point):
    if os.environ.get("DURAFLOW_MESSAGE_FAULT") == point:
        (Path(os.environ["DURAFLOW_MESSAGE_FIXTURE"]) / (point + ".ready")).write_text("ready")
        await asyncio.Event().wait()


class CheckpointStore(PostgresMessageStore):
    async def apply(self, key, message, update):
        if message.kind == "start":
            await checkpoint("before_state_commit")
        from duraflow.messaging import RetryLater

        try:
            result = await super().apply(key, message, update)
        except RetryLater:
            if message.kind == "finish_task":
                (Path(os.environ["DURAFLOW_MESSAGE_FIXTURE"]) / "fenced_finish.ready").write_text("ready")
            raise
        if message.kind == "start":
            await checkpoint("after_state_commit")
        return result


class CheckpointTransport(PulsarTransport):
    async def publish(self, topic, data, properties, **kwargs):
        from duraflow.messaging import Message

        message = Message.from_bytes(data)
        await super().publish(topic, data, properties, **kwargs)
        if message.kind == "activation_result":
            await checkpoint("after_replay_publish")
        if message.kind == "task_result":
            await checkpoint("after_result_publish")


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
    transport = CheckpointTransport(os.environ["DURAFLOW_TEST_PULSAR"])
    store = None
    if role == "engine":
        store = CheckpointStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace)
        runtime = WorkflowEngine(store, transport, ["message-order"], topics=topics)
    elif role == "workflow":
        runtime = WorkflowWorker(
            transport, registry, topics=topics, replay_executor=ProcessReplayExecutor("tests.message_app")
        )
    elif role == "task":
        store = CheckpointStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace + "_tasks")
        runtime = PausingTaskWorker(transport, registry, topics=topics, journal=store, lease_seconds=2)
    elif role == "tags":
        store = PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace + "_tags")
        runtime = TagEngine(store, transport, topics=topics, page_size=1)
    else:
        store = PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=namespace + "_tasks")
        runtime = TaskTagEngine(store, transport, topics=topics, page_size=1)
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    try:
        # Broker health does not imply every previously owned topic has finished
        # reconnecting after a restart. Keep startup bounded but retry link errors.
        async with asyncio.timeout(60):
            while True:
                try:
                    await runtime.prepare()
                    break
                except (
                    TimeoutError,
                    transport.pulsar.Timeout,
                    transport.pulsar.NotConnected,
                    transport.pulsar.ConnectError,
                ):
                    await asyncio.sleep(0.2)
        (Path(os.environ["DURAFLOW_MESSAGE_FIXTURE"]) / f"{role}-{index}.ready").write_text("ready")
        await runtime.run(stop, poll_interval=0.01)
    finally:
        await runtime.close()
        await transport.close()
        if store:
            await store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=("engine", "workflow", "task", "tags", "task-tags"))
    parser.add_argument("index", type=int)
    args = parser.parse_args()
    asyncio.run(main(args.role, args.index))
