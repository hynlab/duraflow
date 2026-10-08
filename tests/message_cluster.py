"""Owned, independently restartable protocol-2 processes and their journals."""

import asyncio
import os
import signal
import sqlite3
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from duraflow import Client, Topics, WorkflowRef
from duraflow.message_postgres import PostgresMessageStore
from duraflow.transport import PulsarTransport
from scripts.pytest_guard import record_resource, terminate_group


async def eventually(predicate, *, timeout=40):
    async with asyncio.timeout(timeout):
        while True:
            result = await predicate()
            if result:
                return result
            await asyncio.sleep(0.05)


class Processes:
    def __init__(self, root, *, engines=2, workflows=1):
        self.root = Path(root)
        self.namespace = "msg_proc_" + uuid4().hex[:10]
        self.run_id = os.getenv("DURAFLOW_TEST_RUN_ID", uuid4().hex)
        self.terminated = set()
        self.topics = Topics(namespace=self.namespace)
        self.url = os.environ["DURAFLOW_TEST_POSTGRES"]
        self.stores = [
            PostgresMessageStore(self.url, schema=self.namespace + suffix) for suffix in ("", "_tasks", "_tags")
        ]
        self.processes = {}
        self.logs = []
        self.transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
        self.client = Client(self.transport, topics=self.topics, timeout=90)
        self.ref = WorkflowRef("message-order", int, int, build_id="message-v1")
        self.engines, self.workflows = engines, workflows

    async def __aenter__(self):
        try:
            for store in self.stores:
                await store.initialize()
            for role, count in (("engine", self.engines), ("workflow", self.workflows)):
                for index in range(count):
                    await self.spawn(role, index)
            return self
        except BaseException:
            await self.__aexit__(*sys.exc_info())
            raise

    async def spawn(self, role, index, point="", *, database_url=None):
        previous = self.processes.get((role, index))
        if previous is not None and previous.poll() is None:
            raise AssertionError(f"{role}/{index} is still running")
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
                "DURAFLOW_TEST_POSTGRES": database_url or self.url,
                "DURAFLOW_MESSAGE_NAMESPACE": self.namespace,
                "DURAFLOW_MESSAGE_FIXTURE": str(self.root),
                "DURAFLOW_MESSAGE_FAULT": point,
                "DURAFLOW_TEST_RUN_ID": self.run_id,
            },
        )
        self.processes[role, index] = process
        record_resource("process", pid=process.pid)
        async with asyncio.timeout(70):
            while not marker.exists():
                if process.poll() is not None:
                    raise AssertionError(f"{role} exited before readiness: {process.returncode}; log: {log.name}")
                await asyncio.sleep(0.05)

    async def kill(self, role, index, *, sig=signal.SIGKILL):
        process = self.processes[role, index]
        if process.pid in self.terminated:
            return
        await asyncio.to_thread(terminate_group, process.pid, self.run_id, sig)
        await asyncio.to_thread(process.wait, timeout=15)
        self.terminated.add(process.pid)
        if sig == signal.SIGKILL:
            record_resource("process", pid=process.pid, released=True)
        if sig == signal.SIGTERM:
            assert process.returncode == 0, f"{role}/{index}: shutdown exit {process.returncode}"

    async def kill_all(self):
        for role, index in self.processes:
            await self.kill(role, index)

    async def checkpoint(self, point):
        async with asyncio.timeout(40):
            while not (self.root / (point + ".ready")).exists():
                await asyncio.sleep(0.05)

    async def waiting(self, handle):
        async def ready():
            state = await handle.describe()
            return any(n["spec"]["kind"] == "channel_next" and n["state"] == "pending" for n in state["nodes"].values())

        await eventually(ready)

    def ledger(self):
        with sqlite3.connect(self.root / "effects.db") as conn:
            return dict(conn.execute("SELECT key, value FROM effects")), dict(
                conn.execute("SELECT key, count(*) FROM calls GROUP BY key")
            )

    async def __aexit__(self, *args):
        from contextlib import AsyncExitStack
        from sqlalchemy import text

        async def close_store(store):
            try:
                async with store.database._transaction() as conn:
                    await conn.execute(text(f'DROP SCHEMA IF EXISTS "{store.database.schema}" CASCADE'))
            finally:
                await store.close()

        async with AsyncExitStack() as cleanup:
            for store in self.stores:
                cleanup.push_async_callback(close_store, store)
            cleanup.push_async_callback(self.transport.close)
            cleanup.push_async_callback(self.client.close)
            for log in self.logs:
                cleanup.callback(log.close)
            cleanup.push_async_callback(self.kill_all)
