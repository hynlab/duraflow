"""Installed connection factories against real SQLite/PostgreSQL and Pulsar."""

import os
from uuid import uuid4

import pytest

from duraflow import RuntimeSettings
from duraflow.connections import open_journal
from tests.test_runtime_configuration import exercise_runtime

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not (os.getenv("DURAFLOW_TEST_POSTGRES") and os.getenv("DURAFLOW_TEST_PULSAR")),
        reason="Native PostgreSQL/Pulsar endpoints required",
    ),
]


@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("configuration", ["code", "environment"])
async def test_native_public_runtime_restart(tmp_path, backend, configuration):
    schema = "runtime_" + uuid4().hex[:12]
    database = os.environ["DURAFLOW_TEST_POSTGRES"] if backend == "postgres" else f"sqlite:///{tmp_path / 'state.db'}"
    journal = database if backend == "postgres" else f"sqlite:///{tmp_path / 'tasks.db'}"
    if configuration == "code":
        settings = RuntimeSettings(
            database_url=database,
            task_journal_url=journal,
            broker_url=os.environ["DURAFLOW_TEST_PULSAR"],
            namespace=schema,
            message_schema=schema,
            poll_interval=0.01,
        )
    else:
        settings = RuntimeSettings.from_environment(
            {
                "DURAFLOW_DATABASE_URL": database,
                "DURAFLOW_TASK_JOURNAL_URL": journal,
                "DURAFLOW_PULSAR_URL": os.environ["DURAFLOW_TEST_PULSAR"],
                "DURAFLOW_NAMESPACE": schema,
                "DURAFLOW_MESSAGE_SCHEMA": schema,
                "DURAFLOW_POLL_INTERVAL": "0.01",
            }
        )
    try:
        await exercise_runtime(settings)
    finally:
        if backend == "postgres":
            from sqlalchemy import text

            store = open_journal(database, schema, settings)
            try:
                async with store.database._transaction() as conn:
                    for name in (schema, schema + "_tasks"):
                        await conn.execute(text(f'DROP SCHEMA IF EXISTS "{name}" CASCADE'))
            finally:
                await store.close()
