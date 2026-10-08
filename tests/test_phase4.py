from __future__ import annotations

import os
import subprocess
import sys
from types import SimpleNamespace
from urllib.parse import quote
from uuid import uuid4

import pytest

from duraflow import LegacyClient as Client, Registry
from duraflow.config import RuntimeSettings
from duraflow.contracts import Conflict
from duraflow.retention import RetentionPolicy
from duraflow.security import AuthorizationError, secret_value, validate_production_connections
from duraflow.testing import LegacyTestEnvironment as TestEnvironment
from duraflow.transport import PulsarTransport
from tests.test_engine import double, sequence


@pytest.fixture
def certificate(tmp_path):
    ca, key = tmp_path / "ca.pem", tmp_path / "ca-key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=duraflow-disposable-test",
            "-keyout",
            str(key),
            "-out",
            str(ca),
        ],
        check=True,
        capture_output=True,
    )
    ca.chmod(0o644)
    return ca


def test_verified_production_policy_and_redacted_representation(certificate):
    url = "postgresql+psycopg://user:password-not-public@database/duraflow?sslmode=verify-full&sslrootcert=" + quote(
        str(certificate), safe=""
    )
    settings = RuntimeSettings(
        production=True,
        database_url=url,
        broker_url="pulsar+ssl://broker:6651",
        pulsar_token="test-secret-token",
        pulsar_tls_ca=str(certificate),
    )
    assert "password-not-public" not in repr(settings)
    assert "test-secret-token" not in repr(settings)
    for bad in (url.replace("verify-full", "require"), url + "&sslmode=disable", url + "&host=other"):
        with pytest.raises(ValueError):
            validate_production_connections(bad, settings.broker_url, settings.pulsar_token, str(certificate))
    with pytest.raises(ValueError):
        validate_production_connections(url, "pulsar://broker:6650", "token", str(certificate))
    with pytest.raises(ValueError):
        validate_production_connections(url, settings.broker_url, None, str(certificate))
    with pytest.raises(ValueError):
        validate_production_connections(url, settings.broker_url, "token", str(certificate) + "-missing")


async def test_production_runtime_validates_only_the_selected_journal(certificate, monkeypatch):
    from dataclasses import replace
    from duraflow import MemoryBroker, Runtime
    from tests.message_app import registry

    settings = RuntimeSettings(
        production=True,
        broker_url="pulsar+ssl://broker:6651",
        pulsar_token="private-token",
        pulsar_tls_ca=str(certificate),
    )
    transport = MemoryBroker()
    monkeypatch.setattr("duraflow.runtime.broker", lambda settings: transport)
    async with Runtime(role="client", settings=settings) as runtime:
        assert runtime.store is None
    with pytest.raises(ValueError, match="PostgreSQL"):
        await Runtime(role="workflow-engine", settings=settings, registry=registry).initialize()
    with pytest.raises(ValueError, match="independent journal"):
        Runtime(role="task-worker", settings=settings, registry=registry)
    with pytest.raises(ValueError, match="PostgreSQL"):
        await Runtime(
            role="task-worker", settings=replace(settings, task_journal_url="sqlite:///tasks.db"), registry=registry
        ).initialize()


def test_secret_source_bounds_permissions_and_no_implicit_fallback(tmp_path):
    secret = tmp_path / "token"
    secret.write_text("correct-token\n")
    secret.chmod(0o600)
    env = {"DURAFLOW_PULSAR_TOKEN_FILE": str(secret)}
    assert secret_value(env, "DURAFLOW_PULSAR_TOKEN") == "correct-token"
    assert RuntimeSettings.from_environment(env).pulsar_token == "correct-token"
    with pytest.raises(ValueError):
        secret_value({**env, "DURAFLOW_PULSAR_TOKEN": "another"}, "DURAFLOW_PULSAR_TOKEN")
    link = tmp_path / "link"
    link.symlink_to(secret)
    with pytest.raises(ValueError):
        secret_value({"TOKEN_FILE": str(link)}, "TOKEN")
    secret.chmod(0o644)
    with pytest.raises(ValueError):
        secret_value(env, "DURAFLOW_PULSAR_TOKEN")
    secret.chmod(0o600)
    secret.write_text("x" * 16385)
    with pytest.raises(ValueError):
        secret_value(env, "DURAFLOW_PULSAR_TOKEN")


def test_database_secret_file_is_not_overridden_by_cli_sqlite_default(tmp_path):
    from duraflow.cli import parser, runtime_settings

    secret = tmp_path / "database"
    secret.write_text("sqlite:///chosen.db")
    secret.chmod(0o600)
    original = dict(os.environ)
    try:
        os.environ.pop("DURAFLOW_DATABASE_URL", None)
        os.environ["DURAFLOW_DATABASE_URL_FILE"] = str(secret)
        settings = runtime_settings(parser().parse_args(["list"]))
        assert settings.database_url == "sqlite:///chosen.db"
    finally:
        os.environ.clear()
        os.environ.update(original)


async def test_pulsar_explicitly_verifies_hostname_and_rejects_insecure_certificates(monkeypatch):
    captured = {}

    class Native:
        def __init__(self, url, **options):
            captured.update(options)

        def close(self):
            pass

    monkeypatch.setitem(sys.modules, "pulsar", SimpleNamespace(Client=Native))
    broker = PulsarTransport("pulsar+ssl://broker:6651", authentication=object(), tls_trust_certs_file_path="ca.pem")
    try:
        assert captured["tls_allow_insecure_connection"] is False
        assert captured["tls_validate_hostname"] is True
        assert captured["tls_trust_certs_file_path"] == "ca.pem"
    finally:
        await broker.close()


async def test_actor_string_does_not_bypass_authorization(monkeypatch):
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 2, request_id="authorization")

        async def deny(action):
            raise AuthorizationError("denied")

        monkeypatch.setattr(env.store, "authorize_control", deny, raising=False)
        for actor in ("admin", "parent", "duraflow_operator"):
            with pytest.raises(AuthorizationError):
                await handle.terminate(actor=actor, reason="not authorized", request_id=actor)
        assert (await handle.describe())["status"] == "PENDING"
        with pytest.raises(AuthorizationError):
            await handle.archive(actor="admin", reason="no", retention=10, safety_horizon=1)


async def test_archive_floors_pending_obligations_and_tombstones(monkeypatch):
    async with TestEnvironment(Registry(sequence, double)) as env:
        monkeypatch.setattr(env.store, "retention_policy", RetentionPolicy(100, 50), raising=False)
        handle = await env.client.start(sequence, 2, request_id="retained")
        with pytest.raises(Conflict):
            await handle.archive(actor="operator", reason="pending", retention=100, safety_horizon=50)
        assert await env.run(handle) == 9
        env.clock.advance(200)
        with pytest.raises(Conflict):
            await handle.archive(actor="operator", reason="too soon", retention=99, safety_horizon=50)
        with pytest.raises(Conflict):
            await handle.archive(actor="operator", reason="wrong horizon", retention=100, safety_horizon=49)
        await handle.archive(actor="operator", reason="expired", retention=100, safety_horizon=50)
        archived = await handle.describe()
        assert archived["archived"] and archived["input"] is None and not archived["nodes"]
        same = await env.client.start(sequence, 2, request_id="retained")
        assert same.run_id == handle.run_id
        assert archived["history"][-1]["principal"] == "trusted-local"


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_POSTGRES"), reason="Native PostgreSQL not configured")
async def test_native_database_roles_enforce_read_only_and_record_real_operator():
    from sqlalchemy import text
    from sqlalchemy.engine import make_url
    from sqlalchemy.exc import DBAPIError
    from duraflow.postgres import PostgresStore

    suffix = uuid4().hex[:12]
    schema, reader, operator = "sec_" + suffix, "read_" + suffix, "oper_" + suffix
    password = uuid4().hex
    url = os.environ["DURAFLOW_TEST_POSTGRES"]
    admin = PostgresStore(url, schema=schema)
    readonly = privileged = None
    created = []
    try:
        await admin.initialize()
        async with admin.engine.begin() as conn:
            for role in (reader, operator):
                await conn.execute(text(f"CREATE ROLE \"{role}\" LOGIN PASSWORD '{password}'"))
                created.append(role)
                await conn.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
                await conn.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"'))
            await conn.execute(text(f'GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA "{schema}" TO "{operator}"'))
        registry = Registry(sequence, double)
        handle = await Client(admin, registry, namespace=suffix).start(sequence, 3, request_id="roles")
        readonly = PostgresStore(
            make_url(url).set(username=reader, password=password).render_as_string(hide_password=False),
            schema=schema,
            control_role=operator,
        )
        privileged = PostgresStore(
            make_url(url).set(username=operator, password=password).render_as_string(hide_password=False),
            schema=schema,
            control_role=operator,
        )
        reader_handle = Client(readonly, registry, namespace=suffix).get_handle(handle.run_id)
        assert (await reader_handle.describe())["status"] == "PENDING"
        with pytest.raises(AuthorizationError):
            await reader_handle.terminate(actor=operator, reason="spoofed", request_id="forged")
        state = await reader_handle.describe()
        with pytest.raises(DBAPIError):
            await readonly.save(state, state["revision"])
        operator_handle = Client(privileged, registry, namespace=suffix).get_handle(handle.run_id)
        await operator_handle.terminate(actor="display-label", reason="approved", request_id="valid")
        state = await operator_handle.describe()
        assert state["status"] == "TERMINATED"
        audit = next(row for row in state["history"] if row["kind"] == "operator_action")
        assert audit["principal"] == operator and audit["actor"] == "display-label"
    finally:
        for store in (readonly, privileged):
            if store is not None:
                await store.close()
        async with admin.engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
            for role in created:
                await conn.execute(text(f'DROP ROLE "{role}"'))
        await admin.close()
