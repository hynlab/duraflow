"""Real physical backup plus archived-WAL recovery to a named restore point."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from duraflow import Client
from duraflow.contracts import NotFound
from scripts.fault_guard import compose, owned_service, project_name, restart
from tests.native_cluster import NativeCluster, eventually, evidence
from tests.native_fault_app import pipeline, registry

pytestmark = [
    pytest.mark.integration,
    pytest.mark.fault,
    pytest.mark.skipif(
        os.getenv("DURAFLOW_ALLOW_DESTRUCTIVE_TESTS") != "isolated-compose",
        reason="Explicit isolated Compose fault-test opt-in required",
    ),
]


async def sql(statement: str) -> str:
    result = await asyncio.to_thread(
        compose,
        "exec",
        "-T",
        "--user",
        "postgres",
        "postgres",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        "duraflow",
        "-d",
        "duraflow",
        "-At",
        "-c",
        statement,
    )
    return result.decode().strip()


def docker(*args: str, timeout=120) -> bytes:
    project_name()
    return subprocess.run(["docker", *args], check=True, capture_output=True, timeout=timeout).stdout


async def test_physical_wal_pitr_reconciles_acknowledged_external_work(tmp_path):
    started = time.monotonic()
    async with NativeCluster(tmp_path) as cluster:
        primary = await asyncio.to_thread(owned_service, "postgres")
        unique = uuid4().hex[:12]
        backup_root = "/tmp/df-pitr-" + unique
        target = "df_restore_" + unique
        marker = "df_marker_" + unique
        recovery_name = "duraflow-pitr-" + unique
        recovery_id = None
        original_url = cluster.url
        archive_configured = False
        try:
            await asyncio.to_thread(
                compose, "exec", "-T", "--user", "postgres", "postgres", "mkdir", "-p", backup_root + "/wal"
            )
            await sql("ALTER SYSTEM SET archive_mode='on'")
            await sql(
                "ALTER SYSTEM SET archive_command='test ! -f "
                + backup_root
                + "/wal/%f && cp %p "
                + backup_root
                + "/wal/%f'"
            )
            archive_configured = True
            await asyncio.to_thread(compose, "restart", "postgres")
            await asyncio.to_thread(restart, "postgres")
            await cluster.begin(missing_last=True)

            async def two_finished():
                state = await cluster.handle.describe()
                return sum(n["state"] == "done" for n in state["nodes"].values()) == 2

            await eventually(two_finished, description="two committed results before physical backup")
            await cluster.kill_all()
            await sql(f"CREATE TABLE public.{marker}(name TEXT PRIMARY KEY)")
            await sql(f"INSERT INTO public.{marker} VALUES ('before')")
            await asyncio.to_thread(
                compose,
                "exec",
                "-T",
                "--user",
                "postgres",
                "postgres",
                "pg_basebackup",
                "-U",
                "duraflow",
                "-D",
                backup_root + "/base",
                "--format=plain",
                "--wal-method=stream",
                "--checkpoint=fast",
                "--no-password",
                timeout=180,
            )
            await sql(f"SELECT pg_create_restore_point('{target}')")
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
            # Ensure the restore point and subsequent transactions reached a
            # complete archive segment, not merely the primary's live pg_wal.
            segment = await sql("SELECT pg_walfile_name(pg_current_wal_lsn())")
            await sql("SELECT pg_switch_wal()")

            async def archived():
                try:
                    await asyncio.to_thread(
                        compose,
                        "exec",
                        "-T",
                        "--user",
                        "postgres",
                        "postgres",
                        "test",
                        "-s",
                        backup_root + "/wal/" + segment,
                    )
                    return True
                except subprocess.CalledProcessError:
                    return False

            await eventually(archived, timeout=90, description="archived target WAL segment")
            destination = tmp_path / "physical-restore"
            destination.mkdir()
            await asyncio.to_thread(docker, "cp", primary + ":" + backup_root + "/base", str(destination / "base"))
            await asyncio.to_thread(docker, "cp", primary + ":" + backup_root + "/wal", str(destination / "wal"))
            (destination / "base" / "recovery.signal").touch()
            # Only disposable test WAL is copied. Never upload it as an artifact.
            (destination / "wal").chmod(0o755)
            for path in (destination / "wal").iterdir():
                if path.is_file():
                    path.chmod(0o644)
            restore_started = time.monotonic()
            recovery_id = (
                (
                    await asyncio.to_thread(
                        docker,
                        "run",
                        "-d",
                        "--name",
                        recovery_name,
                        "--label",
                        "io.duraflow.qualification=" + project_name(),
                        "-p",
                        "127.0.0.1::5432",
                        "-v",
                        str(destination / "base") + ":/var/lib/postgresql/data",
                        "-v",
                        str(destination / "wal") + ":/wal:ro",
                        "postgres:16",
                        "postgres",
                        "-c",
                        "restore_command=cp /wal/%f %p",
                        "-c",
                        "recovery_target_name=" + target,
                        "-c",
                        "recovery_target_action=promote",
                        "-c",
                        "recovery_target_timeline=current",
                        "-c",
                        "archive_mode=off",
                    )
                )
                .decode()
                .strip()
            )
            info = json.loads(await asyncio.to_thread(docker, "inspect", recovery_id))[0]
            port = int(info["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"])
            restored_url = make_url(original_url).set(host="127.0.0.1", port=port).render_as_string(hide_password=False)
            await cluster.switch_database(restored_url)

            async def restored():
                try:
                    async with cluster.store.engine.connect() as conn:
                        return not (await conn.execute(text("SELECT pg_is_in_recovery()"))).scalar_one()
                except Exception:
                    return False

            await eventually(restored, timeout=90, description="physical recovery target and promotion")
            async with cluster.store.engine.connect() as conn:
                markers = (await conn.execute(text(f"SELECT name FROM public.{marker} ORDER BY name"))).scalars().all()
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
                restore_point=target,
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
            if recovery_id is not None:
                info = json.loads(await asyncio.to_thread(docker, "inspect", recovery_id))[0]
                if info["Config"].get("Labels", {}).get("io.duraflow.qualification") != project_name():
                    raise AssertionError("Refusing cleanup of an unowned restore container")
                await asyncio.to_thread(docker, "rm", "-f", recovery_id)
            if archive_configured:
                await sql("ALTER SYSTEM RESET archive_command")
                await sql("ALTER SYSTEM RESET archive_mode")
                await asyncio.to_thread(compose, "restart", "postgres")
                await asyncio.to_thread(restart, "postgres")
