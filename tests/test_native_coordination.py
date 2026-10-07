"""Real multi-client signals and multi-engine composition/control races."""
import asyncio
import os
import sqlite3
import time

import pytest

from duraflow import Client
from duraflow.contracts import Conflict
from tests.native_cluster import NativeCluster, eventually, evidence
from tests.native_coordination_app import BUFFERED, GO, registry, rollover, waiter

pytestmark = [pytest.mark.integration, pytest.mark.fault,
              pytest.mark.skipif(os.getenv('DURAFLOW_ALLOW_DESTRUCTIVE_TESTS') != 'isolated-compose',
                                 reason='Explicit isolated Compose qualification opt-in required')]


async def prepare_target(cluster, definition, value, request_id):
    await cluster.handle.terminate(actor='test', reason='Replace default fixture workload', request_id='fixture-stop')
    cluster.client = Client(cluster.store, registry, namespace=cluster.namespace)
    cluster.handle = await cluster.client.start(definition, value, request_id=request_id, workflow_id=request_id)
    return cluster.handle


async def test_native_concurrent_start_signal_rollover_and_child_uniqueness(tmp_path, monkeypatch):
    started = time.monotonic()
    monkeypatch.setenv('DURAFLOW_FAULT_APP', 'tests.native_coordination_app')
    async with NativeCluster(tmp_path) as cluster:
        handle = await prepare_target(cluster, rollover, {'round': 0, 'value': 7}, 'logical-rollover')
        stores = [cluster.open_store(cluster.url) for _ in range(2)]
        clients = [Client(store, registry, namespace=cluster.namespace) for store in stores]
        try:
            started_handles = await asyncio.gather(*(
                clients[i % 2].start(rollover, {'round': 0, 'value': 7}, request_id='logical-rollover', workflow_id='logical-rollover')
                for i in range(8)))
            assert {h.run_id for h in started_handles} == {handle.run_id}
            await asyncio.gather(*(
                clients[i % 2].signal_workflow('logical-rollover', GO, True, signal_id='same-signal')
                for i in range(12)))
            await handle.signal(BUFFERED, 37, signal_id='carry-forward')
            assert len((await handle.describe())['signals']) == 2
            await cluster.spawn('worker', 0)
            await cluster.spawn('engine', 0)
            await cluster.spawn('engine', 1)
            async def completed():
                current = await cluster.client.current('logical-rollover')
                state = await current.describe()
                if state['status'] in {'BLOCKED', 'FAILED', 'CANCELLED', 'TERMINATED'}:
                    raise AssertionError('Composition did not remain runnable: ' + state['status'])
                return state if state['status'] == 'COMPLETED' else None
            current = await eventually(completed, description='rollover and child completion')
            assert current['result'] == 7 and current['run_id'] != handle.run_id
            old = await handle.describe()
            assert old['status'] == 'CONTINUED' and old['continued_run_id'] == current['run_id']
            remaining = [s for s in current['signals'] if not s['consumed']]
            assert len(remaining) == 1 and remaining[0]['payload'] == 37
            rows = await cluster.store.scan(cluster.namespace)
            children = [r for r in rows if r['manifest']['name'] == 'qualification_child']
            assert len(children) == 1 and children[0]['status'] == 'COMPLETED'
            key = cluster.namespace + '/' + children[0]['nodes']['0.0']['task_id']
            with sqlite3.connect(cluster.root / 'external.sqlite') as conn:
                calls = conn.execute('SELECT count(*) FROM calls WHERE task_key=?', (key,)).fetchone()[0]
            assert calls == 1
            evidence('native_concurrent_signal_rollover_child', started, duplicate_starts=8, duplicate_signals=12,
                     coordinators=2, child_runs=1, child_calls=1, carried_signals=1)
        finally:
            for store in stores:
                await store.close()


async def test_native_completion_termination_race_preserves_terminal_outcome(tmp_path, monkeypatch):
    started = time.monotonic()
    monkeypatch.setenv('DURAFLOW_FAULT_APP', 'tests.native_coordination_app')
    async with NativeCluster(tmp_path) as cluster:
        handle = await prepare_target(cluster, waiter, 0, 'control-race')
        await cluster.spawn('engine', 0)
        await cluster.spawn('engine', 1)
        async def waiting():
            return bool((await handle.describe())['commands'])
        await eventually(waiting, description='durable signal wait')
        outcomes = await asyncio.gather(handle.signal(GO, True, signal_id='race-signal'),
            handle.terminate(actor='operator', reason='Concurrent termination', request_id='race-control'),
            return_exceptions=True)
        assert all(value is None or isinstance(value, Conflict) for value in outcomes)
        async def terminal():
            state = await handle.describe()
            return state if state['status'] in {'COMPLETED', 'TERMINATED'} else None
        state = await eventually(terminal, description='committed terminal result')
        before = (state['status'], state['result'], state['finished_at'])
        with pytest.raises(Conflict):
            await handle.signal(GO, False, signal_id='late-signal')
        await asyncio.sleep(0.2)
        after = await handle.describe()
        assert (after['status'], after['result'], after['finished_at']) == before
        evidence('native_completion_termination_race', started, coordinators=2, outcome=before[0])
