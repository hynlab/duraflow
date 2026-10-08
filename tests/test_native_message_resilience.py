"""Protocol-2 scaling, commit boundaries, rolling restarts and races on real services."""

import asyncio
import os
import signal

import pytest

from duraflow import Conflict, WorkflowFailed
from tests.message_app import APPROVAL
from tests.message_cluster import Processes

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
        reason="Native endpoints required",
    ),
]


@pytest.mark.parametrize("workers", [1, 2, 4])
async def test_concurrent_clients_duplicate_starts_signals_and_worker_scaling(tmp_path, workers):
    async with Processes(tmp_path, workflows=2) as cluster:
        for index in range(workers):
            await cluster.spawn("task", index)
        handles = await asyncio.gather(
            *[cluster.client.start(cluster.ref, i % 4, request_id=f"order-{i % 4}") for i in range(16)]
        )
        for i in range(4):
            group = handles[i::4]
            assert len({h.run_id for h in group}) == 1
            await cluster.waiting(group[0])
        # Keep the workflows nonterminal until every duplicate signal is accepted;
        # new signals to a terminal workflow intentionally return Conflict.
        for index in range(workers):
            await cluster.kill("task", index, sig=signal.SIGTERM)
        await asyncio.gather(*[h.signal(APPROVAL, True, signal_id="approved") for h in handles])
        for index in range(workers):
            await cluster.spawn("task", index)
        assert await asyncio.gather(*[h.result(timeout=60) for h in handles]) == [4 * (i % 4) for i in range(16)]
        with pytest.raises(Conflict):
            await cluster.client.start(cluster.ref, 99, request_id="order-0")
        effects, calls = cluster.ledger()
        assert len(effects) == 8 and set(calls.values()) == {1}


@pytest.mark.parametrize(
    "role,point",
    [
        ("engine", "before_state_commit"),
        ("engine", "after_state_commit"),
        ("workflow", "after_replay_publish"),
        ("task", "after_result_publish"),
    ],
)
async def test_sigkill_at_state_and_publication_boundaries(tmp_path, role, point):
    async with Processes(tmp_path, engines=0, workflows=0) as cluster:
        for service in ("engine", "workflow", "task"):
            await cluster.spawn(service, 0, point if service == role else "")
        handle = await cluster.client.dispatch(cluster.ref, 5, request_id="boundary")
        await cluster.checkpoint(point)
        await cluster.kill(role, 0)
        await cluster.spawn(role, 0)
        await cluster.waiting(handle)
        await handle.signal(APPROVAL, True, signal_id="approved")
        assert await handle.result(timeout=90) == 20
        effects, calls = cluster.ledger()
        assert len(effects) == 2 and set(calls.values()) == {1}


@pytest.mark.parametrize("role", ["engine", "workflow", "task", "tags", "task-tags"])
async def test_graceful_role_restart_preserves_waiting_work(tmp_path, role):
    async with Processes(tmp_path) as cluster:
        for service in ("task", "tags", "task-tags"):
            await cluster.spawn(service, 0)
        handles = [
            await cluster.client.start(cluster.ref, i, request_id=f"rolling-{i}", tags=("rolling",)) for i in range(3)
        ]
        for handle in handles:
            await cluster.waiting(handle)
        await cluster.kill(role, 0, sig=signal.SIGTERM)
        await cluster.spawn(role, 0)
        await cluster.client.signal_tagged(cluster.ref, "rolling", APPROVAL, True, signal_id="approve-all")
        assert await asyncio.gather(*[h.result(timeout=60) for h in handles]) == [0, 4, 8]
        assert len(cluster.ledger()[0]) == 6


async def test_completion_termination_race_and_late_signals(tmp_path):
    async with Processes(tmp_path, workflows=2) as cluster:
        await cluster.spawn("task", 0)
        handles = [await cluster.client.start(cluster.ref, i, request_id=f"race-{i}") for i in range(6)]
        for handle in handles:
            await cluster.waiting(handle)
        responses = await asyncio.gather(
            *[
                action
                for h in handles
                for action in (
                    h.signal(APPROVAL, True, signal_id="racing"),
                    h.terminate(actor="test", reason="race", request_id="stop"),
                )
            ],
            return_exceptions=True,
        )
        assert all(not isinstance(r, BaseException) or isinstance(r, Conflict) for r in responses)
        for h in handles:
            state = await h.describe()
            assert state["status"] in {"COMPLETED", "TERMINATED"}
            if state["status"] == "TERMINATED":
                with pytest.raises(WorkflowFailed):
                    await h.result(timeout=10)
            else:
                assert await h.result(timeout=10) == state["input"] * 4
            with pytest.raises(Conflict):
                await h.signal(APPROVAL, False, signal_id="late")
            assert (await h.describe())["status"] == state["status"]


async def test_suspended_task_recovers_after_process_loss(tmp_path):
    async with Processes(tmp_path) as cluster:
        await cluster.spawn("task", 0, "after_effect")
        handle = await cluster.client.start(cluster.ref, 7, request_id="lease")
        await cluster.checkpoint("after_effect")
        process = cluster.processes["task", 0]
        os.killpg(process.pid, signal.SIGSTOP)
        try:
            await asyncio.sleep(2.2)
            # Redelivery follows a consumer disconnect, not merely lease expiry.
            await cluster.kill("task", 0)
            await cluster.spawn("task", 1)
            await handle.signal(APPROVAL, True, signal_id="approved")
            assert await handle.result(timeout=60) == 28
            effects, calls = cluster.ledger()
            assert len(effects) == 2 and sorted(calls.values()) == [1, 2]
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGCONT)
