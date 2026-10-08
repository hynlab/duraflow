"""Protocol-2 rollback with broker ACKs and external effects intentionally retained.

Recovery explicitly reissues an acknowledged signal with its original identity.
This validates an operator-assisted replay procedure, not automatic reconciliation
of arbitrarily rolled-back journals with advanced broker subscription cursors.
"""

import os

import pytest

from duraflow.message_postgres import PostgresMessageStore
from tests.message_app import APPROVAL
from tests.message_cluster import Processes
from tests.physical_restore import PhysicalRestore, sql

pytestmark = [
    pytest.mark.integration,
    pytest.mark.fault,
    pytest.mark.timeout(300),
    pytest.mark.skipif(
        os.getenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose", reason="Isolated Compose opt-in required"
    ),
]


@pytest.mark.parametrize("task_journal", ["restored", "current"])
async def test_protocol2_physical_restore_and_explicit_signal_replay(tmp_path, task_journal):
    from sqlalchemy import text

    async with PhysicalRestore(tmp_path) as recovery:
        async with Processes(tmp_path) as cluster:
            await cluster.spawn("task", 0)
            handle = await cluster.client.start(cluster.ref, 13, request_id="pitr")
            await cluster.waiting(handle)
            await cluster.kill_all()
            await recovery.snapshot()
            marker = "df_marker_" + cluster.namespace
            await sql(f"CREATE TABLE public.{marker}(value INTEGER)")
            for role, index in list(cluster.processes):
                await cluster.spawn(role, index)
            await handle.signal(APPROVAL, True, signal_id="acknowledged-signal")
            assert await handle.result(timeout=60) == 52
            original_effects, original_calls = cluster.ledger()
            assert len(original_effects) == 2 and set(original_calls.values()) == {1}
            await cluster.kill_all()
            restored_url = await recovery.restore(cluster.url)
            restored = PostgresMessageStore(restored_url, schema=cluster.namespace)
            try:
                async with restored.database.engine.connect() as conn:
                    assert (
                        await conn.execute(text("SELECT to_regclass(:name)"), {"name": "public." + marker})
                    ).scalar_one() is None
                key = cluster.topics.instance(cluster.ref.name, "pitr")
                aggregate = await restored.read(key)
                assert aggregate["runs"][aggregate["current"]]["status"] == "WAITING"
                for role, index in list(cluster.processes):
                    url = cluster.url if role == "task" and task_journal == "current" else restored_url
                    await cluster.spawn(role, index, database_url=url)
                # Broker cursors are newer than the restored workflow. Reissue the
                # acknowledged command; task idempotency keys survive the rollback.
                await handle.signal(APPROVAL, True, signal_id="acknowledged-signal")
                try:
                    result = await handle.result(timeout=90)
                except TimeoutError:
                    state = await restored.read(key)
                    pytest.fail(f"Restored workflow stalled: active={state.get('active')}, runs={state.get('runs')}")
                assert result == 52
                effects, calls = cluster.ledger()
                assert effects == original_effects
                assert sorted(calls.values()) == ([1, 2] if task_journal == "restored" else [1, 1])
                final = await restored.read(key)
                assert final["runs"][final["current"]]["status"] == "COMPLETED"
            finally:
                await cluster.kill_all()
                await restored.close()
                await sql(f"DROP TABLE IF EXISTS public.{marker}")
