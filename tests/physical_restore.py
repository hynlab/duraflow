"""Physical WAL restore tools restricted by the existing isolated Compose guard."""

import asyncio
import json
import subprocess
from uuid import uuid4

from scripts.fault_guard import compose, owned_service, project_name, restart
from tests.message_cluster import eventually
from scripts.pytest_guard import record_resource


async def sql(statement):
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


def docker(*args, timeout=120):
    project_name()
    return subprocess.run(["docker", *args], check=True, capture_output=True, timeout=timeout).stdout


class PhysicalRestore:
    def __init__(self, directory):
        unique = uuid4().hex[:12]
        self.directory = directory
        self.root = "/tmp/df-pitr-" + unique
        self.target = "df_restore_" + unique
        self.name = "duraflow-pitr-" + unique
        self.container = None
        self.configured = False

    async def __aenter__(self):
        self.primary = await asyncio.to_thread(owned_service, "postgres")
        record_resource("pitr", id=self.root, name=self.name)
        try:
            await asyncio.to_thread(
                compose, "exec", "-T", "--user", "postgres", "postgres", "mkdir", "-p", self.root + "/wal"
            )
            await sql("ALTER SYSTEM SET archive_mode='on'")
            self.configured = True
            await sql(f"ALTER SYSTEM SET archive_command='test ! -f {self.root}/wal/%f && cp %p {self.root}/wal/%f'")
            await asyncio.to_thread(compose, "restart", "postgres")
            await asyncio.to_thread(restart, "postgres")
            return self
        except BaseException:
            await self.__aexit__()
            raise

    async def snapshot(self):
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
            self.root + "/base",
            "--format=plain",
            "--wal-method=stream",
            "--checkpoint=fast",
            "--no-password",
            timeout=180,
        )
        await sql(f"SELECT pg_create_restore_point('{self.target}')")

    async def restore(self, original_url):
        from sqlalchemy import text
        from sqlalchemy.engine import make_url
        from duraflow.message_postgres import PostgresMessageStore

        segment = await sql("SELECT pg_walfile_name(pg_current_wal_lsn())")
        await sql("SELECT pg_switch_wal()")

        async def archived():
            try:
                await asyncio.to_thread(
                    compose, "exec", "-T", "--user", "postgres", "postgres", "test", "-s", self.root + "/wal/" + segment
                )
                return True
            except subprocess.CalledProcessError:
                return False

        await eventually(archived, timeout=90)
        destination = self.directory / "physical-restore"
        destination.mkdir()
        await asyncio.to_thread(docker, "cp", self.primary + ":" + self.root + "/base", str(destination / "base"))
        await asyncio.to_thread(docker, "cp", self.primary + ":" + self.root + "/wal", str(destination / "wal"))
        (destination / "base" / "recovery.signal").touch()
        (destination / "wal").chmod(0o755)
        for path in (destination / "wal").iterdir():
            if path.is_file():
                path.chmod(0o644)
        self.container = (
            (
                await asyncio.to_thread(
                    docker,
                    "run",
                    "-d",
                    "--name",
                    self.name,
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
                    "recovery_target_name=" + self.target,
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
        info = json.loads(await asyncio.to_thread(docker, "inspect", self.container))[0]
        port = int(info["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"])
        url = make_url(original_url).set(host="127.0.0.1", port=port).render_as_string(hide_password=False)
        store = PostgresMessageStore(url)

        async def promoted():
            try:
                async with store.database.engine.connect() as conn:
                    return not (await conn.execute(text("SELECT pg_is_in_recovery()"))).scalar_one()
            except Exception:
                return False

        try:
            await eventually(promoted, timeout=90)
        finally:
            await store.close()
        return url

    async def __aexit__(self, *args):
        try:
            ids = (
                (await asyncio.to_thread(docker, "ps", "-aq", "--filter", "name=^" + self.name + "$")).decode().split()
            )
            if ids:
                info = json.loads(await asyncio.to_thread(docker, "inspect", ids[0]))[0]
                assert info["Config"]["Labels"].get("io.duraflow.qualification") == project_name()
                await asyncio.to_thread(docker, "rm", "-f", ids[0])
        finally:
            if self.configured:
                await sql("ALTER SYSTEM RESET archive_command")
                await sql("ALTER SYSTEM RESET archive_mode")
                await asyncio.to_thread(compose, "restart", "postgres")
                await asyncio.to_thread(restart, "postgres")
            # The path is generated by this helper and is inside the guarded
            # disposable primary. WAL/base backups are not test artifacts.
            await asyncio.to_thread(
                compose, "exec", "-T", "--user", "postgres", "postgres", "rm", "-rf", "--", self.root
            )
        record_resource("pitr", id=self.root, name=self.name, released=True)
