"""Test-only native process fixture. Never point it at production services."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from uuid import uuid4

from sqlalchemy import text

from duraflow import Client
from duraflow.postgres import PostgresStore
from duraflow.transport import PulsarTransport
from scripts.fault_guard import owned_service, project_name
from tests.native_fault_app import initialize_ledger, pipeline, registry, topic


async def eventually(predicate, *, timeout=90, description="native condition"):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if hasattr(value, "__await__"):
            value = await value
        if value:
            return value
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {description}")


class NativeCluster:
    def __init__(self, root: Path):
        project_name()
        self.root = root
        self.namespace = "fault_" + uuid4().hex[:12]
        self.schema = self.namespace
        self.url = os.environ["DURAFLOW_TEST_POSTGRES"]
        self.processes = {}
        self.logs = []
        self.store = self.open_store(self.url)
        self.client = Client(self.store, registry, namespace=self.namespace)
        self.broker = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"], receiver_queue_size=4)
        self.handle = None

    def open_store(self, url):
        return PostgresStore(url, schema=self.schema, operation_timeout=5, statement_timeout=1, lock_timeout=0.5)

    async def __aenter__(self):
        await asyncio.to_thread(owned_service, "postgres")
        await asyncio.to_thread(owned_service, "pulsar")
        self.root.mkdir(parents=True, exist_ok=True)
        initialize_ledger(self.root / "external.sqlite")
        await self.store.initialize()
        await self.broker.ensure(topic(self.namespace, "output").name, "verification")
        self.handle = await self.client.start(
            pipeline, {"namespace": self.namespace, "value": 10}, request_id="pipeline"
        )
        return self

    async def spawn(self, role, index, *, point=""):
        key = f"{role}-{index}"
        if key in self.processes and self.processes[key].poll() is None:
            raise AssertionError("Process is already running")
        marker = self.root / f"{key}.ready"
        marker.unlink(missing_ok=True)
        env = {
            **os.environ,
            "DURAFLOW_TEST_POSTGRES": self.url,
            "DURAFLOW_FAULT_SCHEMA": self.schema,
            "DURAFLOW_FAULT_NAMESPACE": self.namespace,
            "DURAFLOW_FAULT_DIRECTORY": str(self.root),
            "DURAFLOW_FAULT_POINT": point,
            "DURAFLOW_FAULT_HANDLER": str(index),
            "PYTHONPATH": str(Path("src").resolve()) + os.pathsep + str(Path(".").resolve()),
        }
        log = (self.root / (key + f"-{len(self.logs)}.log")).open("wb")
        self.logs.append(log)
        process = subprocess.Popen(
            [sys.executable, "-m", "tests.native_fault_process", role, str(index)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.processes[key] = process

        def ready():
            if process.poll() is not None:
                raise AssertionError(f"{key} exited before readiness: {process.returncode}")
            return marker.exists()

        await eventually(ready, timeout=30, description=key + " readiness")

    async def kill(self, key):
        process = self.processes.get(key)
        if process is not None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            await asyncio.to_thread(process.wait, timeout=10)

    async def kill_all(self):
        for key in self.processes:
            await self.kill(key)

    async def begin(self, *, fault_point="", missing_last=False):
        for index in range(2 if missing_last else 3):
            await self.spawn("worker", index, point=fault_point if index == 1 else "")
        await self.spawn("engine", 0)
        await self.spawn("engine", 1)

    async def at(self, point, handler=1):
        await eventually(
            lambda: (self.root / f"{handler}.{point}.ready").exists(), description=f"handler {handler} at {point}"
        )

    def release(self, point, handler=1):
        (self.root / f"{handler}.{point}.release").write_text("continue")

    async def completed(self, timeout=100):
        async def check():
            state = await self.handle.describe()
            if state["status"] in {"FAILED", "BLOCKED", "CANCELLED", "TERMINATED"}:
                raise AssertionError("Unexpected terminal/block state: " + state["status"])
            return state if state["status"] == "COMPLETED" else None

        state = await eventually(check, timeout=timeout, description="workflow completion")
        assert state["result"] == [10, 11, 12]
        logical = [item for item in state["outbox"].values() if item["metadata"]["kind"] == "publication"]
        assert len(logical) == 1 and logical[0]["delivered"]
        assert len([c for c in state["commands"] if c["spec"]["kind"] == "broadcast"]) == 1
        return state

    def ledger(self):
        with sqlite3.connect(self.root / "external.sqlite", timeout=10) as conn:
            effects = conn.execute("SELECT task_key,handler,result FROM effects ORDER BY handler").fetchall()
            counts = dict(conn.execute("SELECT handler,count(*) FROM calls GROUP BY handler").fetchall())
        assert len(effects) == 3 and [r[2] for r in effects] == [10, 11, 12]
        return counts

    async def output_ids(self):
        found = set()
        for _ in range(20):
            item = await self.broker.receive(topic(self.namespace, "output").name, "verification", timeout=0.2)
            if item is None:
                break
            found.add(json.loads(item.properties["duraflow"])["event_id"])
            await self.broker.ack(item)
        assert len(found) == 1
        return found

    async def switch_database(self, url):
        run_id = self.handle.run_id
        await self.store.close()
        self.url = url
        self.store = self.open_store(url)
        self.client = Client(self.store, registry, namespace=self.namespace)
        self.handle = self.client.get_handle(run_id)

    async def __aexit__(self, *args):
        await self.kill_all()
        for log in self.logs:
            log.close()
        await self.broker.close()
        try:
            async with self.store.engine.begin() as conn:
                await conn.execute(text(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE'))
        finally:
            await self.store.close()


def evidence(case, started, **details):
    path = Path("qualification-results.json")
    data = json.loads(path.read_text()) if path.exists() else {"cases": []}
    data["cases"].append(
        {
            "case": case,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "completed_at_epoch": time.time(),
            **details,
        }
    )
    path.write_text(json.dumps(data, indent=2, sort_keys=True))
