"""Public configuration, connection ownership, and SQLite runtime recovery."""

import asyncio
from contextlib import AsyncExitStack
from unittest.mock import AsyncMock

import pytest

from duraflow import MemoryBroker, Runtime, RuntimeSettings, SQLiteMessageStore
from duraflow.cli import parser, runtime_settings
from duraflow.connections import open_journal
from duraflow.messaging import Message, Publication
from tests.message_app import APPROVAL, order, registry


def test_code_settings_do_not_read_environment(monkeypatch):
    monkeypatch.setenv("DURAFLOW_DATABASE_URL", "invalid://environment-secret")
    monkeypatch.setenv("DURAFLOW_PULSAR_URL", "invalid://environment-secret")
    settings = RuntimeSettings(database_url="sqlite:///code.db", task_journal_url="sqlite:///private-tasks.db")
    assert settings.database_url == "sqlite:///code.db"
    assert settings.broker_url == "pulsar://localhost:6650"
    assert "private-tasks" not in repr(settings)


def test_environment_and_cli_precedence_and_routing(monkeypatch):
    env = {
        "DURAFLOW_DATABASE_URL": "sqlite:///env.db",
        "DURAFLOW_TASK_JOURNAL_URL": "sqlite:///env-tasks.db",
        "DURAFLOW_PULSAR_URL": "pulsar://env:6650",
        "DURAFLOW_MESSAGE_SCHEMA": "env_schema",
        "DURAFLOW_PULSAR_TENANT": "tenant",
        "DURAFLOW_PULSAR_NAMESPACE": "physical",
        "DURAFLOW_NAMESPACE": "logical",
        "DURAFLOW_CONCURRENCY": "invalid-but-overridden",
    }
    settings = RuntimeSettings.from_environment(env, concurrency=3)
    assert settings.task_journal_url == "sqlite:///env-tasks.db"
    assert settings.message_schema == "env_schema"
    assert settings.pulsar_tenant == "tenant" and settings.pulsar_namespace == "physical"
    assert settings.namespace == "logical" and settings.concurrency == 3
    # Parser defaults must not mask an explicitly supplied environment mapping.
    monkeypatch.setenv("DURAFLOW_NAMESPACE", "ambient")
    args = parser().parse_args(["--database", "sqlite:///cli.db", "--concurrency", "4", "health"])
    configured = runtime_settings(args, environment=env)
    assert configured.database_url == "sqlite:///cli.db"
    assert configured.broker_url == "pulsar://env:6650"
    assert configured.namespace == "logical" and configured.concurrency == 4


def test_role_specific_secret_files_and_overrides(tmp_path, monkeypatch):
    secret = tmp_path / "tasks-url"
    secret.write_text("sqlite:///secret-tasks.db\n")
    secret.chmod(0o600)
    env = {
        "DURAFLOW_DATABASE_URL_FILE": str(tmp_path / "missing"),
        "DURAFLOW_TASK_JOURNAL_URL_FILE": str(secret),
    }
    task = RuntimeSettings.from_environment(env, role="task-worker")
    assert task.task_journal_url == "sqlite:///secret-tasks.db"
    client = RuntimeSettings.from_environment(env, role="client")
    assert client.task_journal_url is None
    overridden = RuntimeSettings.from_environment(env, database_url="sqlite:///override.db")
    assert overridden.database_url == "sqlite:///override.db"
    with pytest.raises(ValueError, match="secret source"):
        RuntimeSettings.from_environment(env, role="workflow-engine")
    with pytest.raises(ValueError, match="Unknown runtime role"):
        RuntimeSettings.from_environment(env, role="typo")
    monkeypatch.setenv("DURAFLOW_TASK_JOURNAL_URL_FILE", str(secret))
    monkeypatch.delenv("DURAFLOW_TASK_JOURNAL_URL", raising=False)
    assert RuntimeSettings.from_environment(role="task-worker").task_journal_url == task.task_journal_url


@pytest.mark.parametrize(
    "url", ["mysql://secret@host/db", "sqlite:///", "sqlite:///db?mode=memory", "sqlite:///:memory:"]
)
def test_journal_rejects_unsupported_urls_without_disclosing_them(url):
    with pytest.raises(ValueError) as error:
        open_journal(url, "test", RuntimeSettings())
    assert "secret@host" not in str(error.value)


async def test_sqlite_relative_and_absolute_paths_and_restart(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    settings = RuntimeSettings()
    for url, filename in [
        ("sqlite:///relative.db", "relative.db"),
        (f"sqlite:///{tmp_path / 'absolute.db'}", "absolute.db"),
    ]:
        first = open_journal(url, "test", settings)
        message = Message("test", "key", {})
        assert await first.apply("key", message, lambda state, now: state.update(value=42) or [])
        await first.close()
        assert (tmp_path / filename).is_file()
        second = open_journal(url, "test", settings)
        assert await second.read("key") == {"value": 42}
        assert not await second.apply("key", message, lambda state, now: [])
        await second.close()


async def test_sqlite_competing_connections_deduplicate_and_claim_atomically(tmp_path):
    stores = [SQLiteMessageStore(tmp_path / "shared.db") for _ in range(2)]
    incoming = Message("test", "key", {})
    outgoing = Message("next", "key", {})

    def update(state, now):
        state["count"] = state.get("count", 0) + 1
        return [Publication("topic", outgoing)]

    results = await asyncio.gather(*(store.apply("key", incoming, update) for store in stores))
    assert sorted(results) == [False, True]
    claims = await asyncio.gather(*(store.claim(f"owner-{i}") for i, store in enumerate(stores)))
    assert sum(map(len, claims)) == 1
    assert await stores[0].read("key") == {"count": 1}


async def exercise_runtime(settings):
    """Run all public roles, persist a waiting workflow, then reconnect its engine."""
    stop = asyncio.Event()
    engine_stop = asyncio.Event()
    async with AsyncExitStack() as stack:
        services = []
        for role in ("workflow-worker", "task-worker", "tag-engine", "task-tag-engine"):
            runtime = Runtime(role=role, settings=settings, app="tests.message_app")
            await runtime.initialize()
            services.append(await stack.enter_async_context(runtime))
        client_runtime = await stack.enter_async_context(Runtime(role="client", settings=settings, registry=registry))
        assert client_runtime.store is None and services[0].store is None
        engine = Runtime(role="workflow-engine", settings=settings, registry=registry)
        await engine.initialize()
        async with asyncio.TaskGroup() as tasks:
            for service in services:
                tasks.create_task(service.run(stop=stop))
            try:
                async with engine:
                    running = tasks.create_task(engine.run(stop=engine_stop))
                    handle = await client_runtime.client.start(order, 7, request_id="runtime-order", tags=("orders",))
                    async with asyncio.timeout(20):
                        while not (await handle.describe())["channels"]:
                            await asyncio.sleep(0.02)
                    engine_stop.set()
                    await running
                # The replacement reconnects to the same journal and broker routes.
                async with Runtime(role="workflow-engine", settings=settings, registry=registry) as replacement:
                    restarted = tasks.create_task(replacement.run(stop=stop))
                    try:
                        await handle.signal(APPROVAL, True, signal_id="after-restart")
                        assert await handle.result(timeout=20) == 28
                    finally:
                        stop.set()
                        await restarted
            finally:
                engine_stop.set()
                stop.set()


async def test_public_runtime_with_sqlite_and_retained_test_broker(tmp_path, monkeypatch):
    transport = MemoryBroker()
    monkeypatch.setattr("duraflow.runtime.broker", lambda settings: transport)
    await exercise_runtime(
        RuntimeSettings(
            database_url=f"sqlite:///{tmp_path / 'workflows.db'}",
            task_journal_url=f"sqlite:///{tmp_path / 'tasks.db'}",
            poll_interval=0.005,
            probe_interval=0.05,
        )
    )


async def test_initialization_does_not_connect_to_broker_and_failed_enter_closes_journal(tmp_path, monkeypatch):
    store = SQLiteMessageStore(tmp_path / "state.db")
    store.close = AsyncMock()
    monkeypatch.setattr("duraflow.runtime.open_journal", lambda *args: store)
    transport = MemoryBroker()
    transport.ensure = AsyncMock(side_effect=RuntimeError("subscription failure"))
    transport.close = AsyncMock()
    connect = []

    def broker(settings):
        connect.append(True)
        return transport

    monkeypatch.setattr("duraflow.runtime.broker", broker)
    runtime = Runtime(role="workflow-engine", registry=registry)
    await runtime.initialize()
    assert connect == []
    store.close.assert_awaited_once()
    store.close.reset_mock()
    with pytest.raises(RuntimeError, match="subscription failure"):
        async with runtime:
            pytest.fail("Unreachable")
    store.close.assert_awaited_once()
    transport.close.assert_awaited_once()
    with pytest.raises(RuntimeError, match="single-use"):
        await runtime.__aenter__()


async def test_runtime_cancellation_closes_owned_resources_once(tmp_path, monkeypatch):
    store = SQLiteMessageStore(tmp_path / "state.db")
    store.close = AsyncMock()
    transport = MemoryBroker()
    transport.close = AsyncMock()
    monkeypatch.setattr("duraflow.runtime.open_journal", lambda *args: store)
    monkeypatch.setattr("duraflow.runtime.broker", lambda settings: transport)
    close_consumer = AsyncMock()
    monkeypatch.setattr("duraflow.runtime.WorkflowEngine.close", close_consumer)
    runtime = Runtime(role="workflow-engine", registry=registry)
    with pytest.raises(RuntimeError, match="Enter a service"):
        await runtime.run()
    async with runtime:
        running = asyncio.create_task(runtime.run())
        await asyncio.sleep(0.02)
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
    close_consumer.assert_awaited_once()
    store.close.assert_awaited_once()
    transport.close.assert_awaited_once()
