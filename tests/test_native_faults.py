"""Actual process kills, broker outages and database outages; explicit opt-in only."""

from __future__ import annotations

import asyncio
import os
import time

import pytest

from scripts.fault_guard import interrupt, restart

pytestmark = [
    pytest.mark.integration,
    pytest.mark.fault,
    pytest.mark.skipif(
        os.getenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose",
        reason="Explicit isolated Compose fault-test opt-in required",
    ),
]


@pytest.mark.parametrize("point", ["before_effect", "after_effect", "after_observation"])
async def test_real_worker_sigkill_at_commit_boundaries(tmp_path, point):
    from tests.native_cluster import NativeCluster, evidence

    started = time.monotonic()
    async with NativeCluster(tmp_path) as cluster:
        await cluster.begin(fault_point=point)
        await cluster.at(point)
        await cluster.kill("worker-1")
        assert cluster.processes["worker-1"].returncode < 0
        await cluster.spawn("worker", 1)
        await cluster.completed()
        counts = cluster.ledger()
        assert counts[0] == counts[2] == 1
        assert counts[1] == (1 if point == "after_observation" else 2)
        await cluster.output_ids()
        evidence("worker_sigkill_" + point, started, calls=counts, effects=3, coordinators=2)


async def test_real_coordinator_sigkill_preserves_partial_join(tmp_path):
    from tests.native_cluster import NativeCluster, eventually, evidence

    started = time.monotonic()
    async with NativeCluster(tmp_path) as cluster:
        await cluster.begin(missing_last=True)

        async def first_two_done():
            state = await cluster.handle.describe()
            return sum(n["state"] == "done" for n in state["nodes"].values()) == 2

        await eventually(first_two_done, description="two committed handler results")
        await cluster.kill_all()
        assert (await cluster.handle.describe())["status"] != "COMPLETED"
        await cluster.begin()
        await cluster.completed()
        assert cluster.ledger() == {0: 1, 1: 1, 2: 1}
        await cluster.output_ids()
        evidence("coordinator_sigkill_partial_join", started, effects=3, coordinators=2)


@pytest.mark.parametrize("service", ["postgres", "pulsar"])
async def test_real_native_service_sigkill_and_reconnect(tmp_path, service):
    from tests.native_cluster import NativeCluster, evidence

    started = time.monotonic()
    async with NativeCluster(tmp_path) as cluster:
        await cluster.begin(fault_point="after_effect")
        await cluster.at("after_effect")
        try:
            await asyncio.to_thread(interrupt, service)
            cluster.release("after_effect")
            # Real elapsed time, not a mocked lease clock. This deliberately
            # exceeds the worker lease during the PostgreSQL outage.
            await asyncio.sleep(3)
        finally:
            await asyncio.to_thread(restart, service)
        await cluster.completed(timeout=120)
        counts = cluster.ledger()
        assert counts[0] == counts[2] == 1
        assert counts[1] >= 1
        if service == "postgres":
            assert counts[1] >= 2
        await cluster.output_ids()
        evidence(service + "_sigkill_reconnect", started, calls=counts, effects=3, coordinators=2)
