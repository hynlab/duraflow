"""Real SIGKILL qualification of the complete message loop and external effects."""

import asyncio
import os
import sqlite3

import pytest

from tests.message_cluster import Processes
from tests.message_app import APPROVAL

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
        reason="Native endpoints required",
    ),
]


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
