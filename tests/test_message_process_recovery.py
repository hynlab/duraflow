"""Real SIGKILL qualification of the complete message loop and external effects."""

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from duraflow import Client, Topics, WorkflowRef
from duraflow.message_postgres import PostgresMessageStore
from duraflow.transport import PulsarTransport
from tests.message_app import APPROVAL

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
        reason="Native endpoints required",
    ),
]


class Processes:
    def __init__(self, root):
        self.root = root
        self.namespace = "msg_proc_" + uuid4().hex[:10]
        self.topics = Topics(namespace=self.namespace)
        self.stores = [
            PostgresMessageStore(os.environ["DURAFLOW_TEST_POSTGRES"], schema=self.namespace + suffix)
            for suffix in ("", "_tasks")
        ]
        self.processes = {}
        self.logs = []
        self.transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
        self.client = Client(self.transport, topics=self.topics)
        self.ref = WorkflowRef("message-order", int, int, build_id="message-v1")

    async def __aenter__(self):
        for store in self.stores:
            await store.initialize()
        for role, index in (("engine", 0), ("engine", 1), ("workflow", 0)):
            await self.spawn(role, index)
        return self

    async def spawn(self, role, index, point=""):
        marker = self.root / f"{role}-{index}.ready"
        marker.unlink(missing_ok=True)
        log = (self.root / f"{role}-{index}-{len(self.logs)}.log").open("wb")
        self.logs.append(log)
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.message_process", role, str(index)],
            start_new_session=True,
            stdout=log,
            stderr=subprocess.STDOUT,
            env={
                **os.environ,
                "PYTHONPATH": str(Path("src").resolve()) + os.pathsep + str(Path.cwd()),
                "DURAFLOW_MESSAGE_NAMESPACE": self.namespace,
                "DURAFLOW_MESSAGE_FIXTURE": str(self.root),
                "DURAFLOW_MESSAGE_FAULT": point,
            },
        )
        self.processes[role, index] = process
        async with asyncio.timeout(30):
            while not marker.exists():
                if process.poll() is not None:
                    raise AssertionError(f"{role} exited before readiness: {process.returncode}")
                await asyncio.sleep(0.05)

    async def kill(self, role, index):
        process = self.processes[role, index]
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
        await asyncio.to_thread(process.wait, timeout=10)

    async def __aexit__(self, *args):
        from sqlalchemy import text

        for role, index in self.processes:
            await self.kill(role, index)
        for log in self.logs:
            log.close()
        await self.client.close()
        await self.transport.close()
        for store in self.stores:
            try:
                async with store.database._transaction() as conn:
                    await conn.execute(text(f'DROP SCHEMA IF EXISTS "{store.database.schema}" CASCADE'))
            finally:
                await store.close()


@pytest.mark.parametrize("point", ["before_effect", "after_effect", "after_observation"])
async def test_message_task_sigkill_recovers_without_duplicate_accepted_effect(tmp_path, point):
    async with Processes(tmp_path) as cluster:
        await cluster.spawn("task", 0, point)
        handle = await cluster.client.start(cluster.ref, 7, request_id="fault")
        marker = tmp_path / (point + ".ready")
        async with asyncio.timeout(30):
            while not marker.exists():
                await asyncio.sleep(0.05)
        await cluster.kill("task", 0)
        await asyncio.sleep(2.1)
        await cluster.spawn("task", 0)
        await handle.signal(APPROVAL, True, signal_id="approved")
        assert await handle.result(timeout=40) == 28
        state = await handle.describe()
        key = cluster.namespace + "/" + state["nodes"]["1.0"]["task_id"]
        with sqlite3.connect(tmp_path / "effects.db") as conn:
            assert conn.execute("SELECT count(*) FROM effects WHERE key=?", (key,)).fetchone()[0] == 1
            assert conn.execute("SELECT count(*) FROM calls WHERE key=?", (key,)).fetchone()[0] == (
                1 if point == "after_observation" else 2
            )


async def test_both_message_state_engines_sigkill_resume_buffered_signal(tmp_path):
    async with Processes(tmp_path) as cluster:
        await cluster.spawn("task", 0)
        handle = await cluster.client.start(cluster.ref, 3, request_id="engine-restart")
        async with asyncio.timeout(30):
            while not any(n["spec"]["kind"] == "channel_next" for n in (await handle.describe())["nodes"].values()):
                await asyncio.sleep(0.05)
        await cluster.kill("engine", 0)
        await cluster.kill("engine", 1)
        signaling = asyncio.create_task(handle.signal(APPROVAL, True, signal_id="during-outage"))
        try:
            await asyncio.sleep(0.2)
            assert not signaling.done()
            await cluster.spawn("engine", 0)
            await cluster.spawn("engine", 1)
            await signaling
            assert await handle.result(timeout=30) == 12
        finally:
            if not signaling.done():
                signaling.cancel()
                await asyncio.gather(signaling, return_exceptions=True)
