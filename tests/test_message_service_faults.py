"""Protocol-2 outages of explicitly owned disposable PostgreSQL/Pulsar services."""

import asyncio
import os
import signal

import pytest

from scripts.fault_guard import compose, interrupt, owned_service, restart, wait_healthy
from scripts.pytest_guard import record_resource
from tests.message_app import APPROVAL
from tests.message_cluster import Processes, eventually

pytestmark = [
    pytest.mark.integration,
    pytest.mark.fault,
    pytest.mark.skipif(
        os.getenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose", reason="Isolated Compose opt-in required"
    ),
]


@pytest.mark.parametrize("service", ["postgres", "pulsar"])
@pytest.mark.parametrize("mode", ["kill", "pause"])
async def test_service_outage_and_worker_restart_preserve_committed_effect(tmp_path, service, mode):
    await asyncio.to_thread(owned_service, service)
    async with Processes(tmp_path) as cluster:
        await cluster.spawn("task", 0)
        handle = await cluster.client.start(cluster.ref, 11, request_id="outage")
        await cluster.waiting(handle)
        effects_before, _ = cluster.ledger()
        assert len(effects_before) == 1
        interrupted = False
        try:
            if mode == "kill":
                await asyncio.to_thread(interrupt, service)
            else:
                record_resource("service", service=service)
                await asyncio.to_thread(compose, "pause", service)
            interrupted = True
            await cluster.kill("task", 0)
            await asyncio.sleep(2)
        finally:
            if interrupted:
                if mode == "pause":
                    await asyncio.to_thread(compose, "unpause", service)
                    await asyncio.to_thread(wait_healthy, service)
                else:
                    await asyncio.to_thread(restart, service)
        await cluster.spawn("task", 0)
        await handle.signal(APPROVAL, True, signal_id="after-outage")
        assert await handle.result(timeout=90) == 44
        effects, calls = cluster.ledger()
        assert len(effects) == 2 and set(calls.values()) == {1}
        assert effects.items() >= effects_before.items()


async def test_live_stale_owner_cannot_overwrite_replacement_after_topic_unload(tmp_path):
    from duraflow.contracts import canonical

    await asyncio.to_thread(owned_service, "pulsar")
    async with Processes(tmp_path) as cluster:
        await cluster.spawn("task", 0, "after_effect")
        handle = await cluster.client.start(cluster.ref, 7, request_id="live-owner")
        await cluster.checkpoint("after_effect")
        journal = cluster.stores[1]
        states = await journal.list_states(canonical([cluster.namespace])[:-1])
        original = next(state for state in states if "request" in state)
        request = original["request"]
        key = canonical([cluster.namespace, "task", request["dispatch_id"]])
        process = cluster.processes["task", 0]
        os.killpg(process.pid, signal.SIGSTOP)
        try:
            await asyncio.sleep(2.2)
            topic = cluster.topics.task(request["ref"]["name"], request["ref"]["version"])
            await asyncio.to_thread(compose, "exec", "-T", "pulsar", "bin/pulsar-admin", "topics", "unload", topic)
            await cluster.spawn("task", 1)

            async def taken_over():
                state = await journal.read(key)
                return state if state.get("generation", 0) > original["generation"] and "outcome" in state else None

            replacement = await eventually(taken_over)
            (tmp_path / "after_effect.release").write_text("continue")
            os.killpg(process.pid, signal.SIGCONT)
            await cluster.checkpoint("fenced_finish")
            fenced = await journal.read(key)
            assert fenced["generation"] == replacement["generation"]
            assert fenced["outcome"] == replacement["outcome"]
            await handle.signal(APPROVAL, True, signal_id="approved")
            assert await handle.result(timeout=60) == 28
            effects, calls = cluster.ledger()
            assert len(effects) == 2 and sorted(calls.values()) == [1, 2]
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGCONT)
