"""Legacy-runtime physical WAL recovery, sharing the owned restore harness."""

import os
import time
from uuid import uuid4

import pytest

from duraflow import LegacyClient as Client
from duraflow.contracts import NotFound
from tests.native_fault_app import pipeline, registry
from tests.physical_restore import PhysicalRestore, sql

pytestmark = [
    pytest.mark.integration,
    pytest.mark.fault,
    pytest.mark.timeout(300),
    pytest.mark.skipif(
        os.getenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose",
        reason="Explicit isolated Compose fault-test opt-in required",
    ),
]


async def test_physical_wal_pitr_reconciles_acknowledged_external_work(tmp_path):
    from sqlalchemy import text
    from tests.native_cluster import NativeCluster, eventually, evidence

    started = time.monotonic()
    async with PhysicalRestore(tmp_path) as recovery:
        async with NativeCluster(tmp_path) as cluster:
            original_url = cluster.url
            marker = "df_marker_" + uuid4().hex[:12]
            try:
                await cluster.begin(missing_last=True)

                async def two_finished():
                    state = await cluster.handle.describe()
                    return sum(n["state"] == "done" for n in state["nodes"].values()) == 2

                await eventually(two_finished, description="two committed results before physical backup")
                await cluster.kill_all()
                await sql(f"CREATE TABLE public.{marker}(name TEXT PRIMARY KEY)")
                await sql(f"INSERT INTO public.{marker} VALUES ('before')")
                await recovery.snapshot()
                target_time = float(await sql("SELECT EXTRACT(EPOCH FROM clock_timestamp())"))
                await sql(f"INSERT INTO public.{marker} VALUES ('after')")
                lost_client = Client(cluster.store, registry, namespace=cluster.namespace + "_lost")
                lost = await lost_client.start(
                    pipeline, {"namespace": cluster.namespace, "value": 99}, request_id="after-target"
                )
                await cluster.begin()
                original_state = await cluster.completed()
                assert cluster.ledger() == {0: 1, 1: 1, 2: 1}
                await cluster.kill_all()
                restore_started = time.monotonic()
                restored_url = await recovery.restore(original_url)
                await cluster.switch_database(restored_url)
                async with cluster.store.engine.connect() as conn:
                    markers = (
                        (await conn.execute(text(f"SELECT name FROM public.{marker} ORDER BY name"))).scalars().all()
                    )
                assert markers == ["before"]
                with pytest.raises(NotFound):
                    await cluster.store.load(cluster.namespace + "_lost", lost.run_id)
                restored_state = await cluster.handle.describe()
                assert restored_state["status"] != "COMPLETED"
                assert sum(n["state"] == "done" for n in restored_state["nodes"].values()) == 2
                await cluster.begin()
                recovered_state = await cluster.completed()
                assert cluster.ledger() == {0: 1, 1: 1, 2: 2}
                original_publications = {
                    key for key, item in original_state["outbox"].items() if item["metadata"]["kind"] == "publication"
                }
                recovered_publications = {
                    key for key, item in recovered_state["outbox"].items() if item["metadata"]["kind"] == "publication"
                }
                assert recovered_publications == original_publications
                await cluster.output_ids()
                evidence(
                    "physical_wal_pitr",
                    started,
                    restore_seconds=round(time.monotonic() - restore_started, 3),
                    restore_point=recovery.target,
                    target_epoch=target_time,
                    deliberately_lost_post_target_runs=1,
                    effects=3,
                    calls={0: 1, 1: 1, 2: 2},
                    broker_rolled_back=False,
                    external_ledger_rolled_back=False,
                )
            finally:
                await cluster.kill_all()
                if cluster.url != original_url:
                    await cluster.switch_database(original_url)
                await sql(f"DROP TABLE IF EXISTS public.{marker}")
