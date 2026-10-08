from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from types import SimpleNamespace

import pytest

from duraflow import Registry
from duraflow.config import RuntimeSettings
from duraflow.contracts import Conflict, ProtocolError
from duraflow.observability import HealthState, JsonLogFormatter, ProbeServer, render_metrics, sample_health
from duraflow.quarantine import decode_quarantine, quarantine, replay_quarantine
from duraflow.supervision import ShutdownTimeout, supervise
from duraflow.testing import LegacyTestEnvironment as TestEnvironment
from duraflow.transport import MemoryTransport, PulsarTransport
from tests.test_engine import DOUBLE, double, sequence
from tests.test_recovery import dispatch_first


@pytest.mark.parametrize(
    "kwargs",
    [
        {"concurrency": 0},
        {"lease_seconds": 1},
        {"replay_timeout": float("nan")},
        {"poll_interval": None},
        {"production": True},
        {"probe_port": 70000},
    ],
)
def test_runtime_settings_reject_invalid_limits(kwargs):
    with pytest.raises((ValueError, TypeError)):
        RuntimeSettings(**kwargs)


def test_settings_hide_connection_secrets_and_parse_env():
    settings = RuntimeSettings.from_environment(
        {"DURAFLOW_CONCURRENCY": "12", "DURAFLOW_DATABASE_URL": "sqlite:///secret"}, concurrency=4
    )
    assert settings.concurrency == 4 and "secret" not in repr(settings)
    with pytest.raises(ValueError):
        RuntimeSettings.from_environment({"DURAFLOW_PRODUCTION": "sometimes"})


def test_log_formatter_never_serializes_arbitrary_error_text_or_payloads():
    record = logging.LogRecord("duraflow", logging.ERROR, "", 1, "supersecrettoken", ("password",), None)
    record.token = "token-do-not-log"
    record.error_type = "ConnectionError"
    record.run_id = "run-123"
    rendered = JsonLogFormatter().format(record)
    assert "supersecrettoken" not in rendered and "password" not in rendered and "token-do-not-log" not in rendered
    assert json.loads(rendered)["run_id"] == "run-123"


async def http(port, path):
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(f"GET {path} HTTP/1.0\r\n\r\n".encode())
    await writer.drain()
    data = await reader.read()
    writer.close()
    await writer.wait_closed()
    return data


async def test_probes_distinguish_liveness_readiness_drain_and_staleness():
    health = HealthState(ready=True, checked_at=time.monotonic())
    runtime = SimpleNamespace(metrics={"activations": 7})
    server = ProbeServer(health, runtime, "engine", max_age=1)
    port = await server.start("127.0.0.1", 0)
    try:
        assert b"200" in (await http(port, "/live")).splitlines()[0]
        assert b"200" in (await http(port, "/ready")).splitlines()[0]
        health.draining = True
        assert b"503" in (await http(port, "/ready")).splitlines()[0]
        assert b"200" in (await http(port, "/live")).splitlines()[0]
        health.draining = False
        health.checked_at -= 10
        assert b"503" in (await http(port, "/ready")).splitlines()[0]
        assert b"duraflow_activations_total" in await http(port, "/metrics")
        assert b"404" in (await http(port, "/cancel")).splitlines()[0]
    finally:
        await server.close()
    assert "run_id" not in render_metrics(runtime, health, "engine")


async def test_worker_refuses_new_delivery_after_drain():
    async with TestEnvironment(Registry(sequence, double)) as env:
        await env.client.start(sequence, 2, request_id="drain")
        delivery = await dispatch_first(env)
        env.worker.draining = True
        await env.worker.process(delivery, DOUBLE)
        assert env.worker.metrics["executed"] == 0
        env.worker.draining = False
        assert await env.worker.step()
        assert env.worker.metrics["executed"] == 1


class Runtime:
    def __init__(self, stubborn=False):
        self.metrics = {}
        self.started = asyncio.Event()
        self.closed = False
        self.stubborn = stubborn

    async def run(self, stop, **kwargs):
        self.started.set()
        await stop.wait()
        if self.stubborn:
            await asyncio.sleep(100)

    async def close(self):
        self.closed = True


async def test_supervision_drains_and_reports_dependency_readiness():
    async with TestEnvironment(Registry(sequence, double)) as env:
        runtime = Runtime()
        stop = asyncio.Event()
        settings = RuntimeSettings(probe_interval=0.02, probe_timeout=0.1, shutdown_timeout=0.2)
        running = asyncio.create_task(
            supervise(runtime, MemoryTransport(), env.store, env.client.registry, settings, stop, role="worker")
        )
        await runtime.started.wait()
        for _ in range(50):
            if runtime.accepting:
                break
            await asyncio.sleep(0.005)
        assert runtime.accepting
        stop.set()
        await asyncio.wait_for(running, 2)
        assert runtime.closed and runtime.draining and not runtime.accepting


async def test_library_supervision_times_out_without_killing_host():
    async with TestEnvironment(Registry(sequence, double)) as env:
        runtime, stop = Runtime(stubborn=True), asyncio.Event()
        settings = RuntimeSettings(shutdown_timeout=0.03, probe_timeout=0.01)
        running = asyncio.create_task(
            supervise(runtime, MemoryTransport(), env.store, env.client.registry, settings, stop, role="worker")
        )
        await runtime.started.wait()
        stop.set()
        with pytest.raises(ShutdownTimeout):
            await asyncio.wait_for(running, 2)
        assert runtime.closed


async def test_valid_quarantine_requeues_only_pending_committed_work():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 2, request_id="dlq")
        delivery = await dispatch_first(env)
        raw = quarantine(delivery, "operator-test")
        assert decode_quarantine(raw)["properties"] == delivery.properties
        assert await replay_quarantine(env.client, raw, actor="operator", reason="fixed", request_id="retry")
        assert not await replay_quarantine(env.client, raw, actor="operator", reason="fixed", request_id="retry")
        assert await env.run(handle) == 9
        assert not await replay_quarantine(env.client, raw, actor="operator", reason="already done", request_id="again")
        assert env.worker.metrics["executed"] == 2
        with pytest.raises(Conflict):
            await replay_quarantine(env.client, raw, actor="other", reason="fixed", request_id="retry")


async def test_forged_quarantine_cannot_change_committed_payload():
    async with TestEnvironment(Registry(sequence, double)) as env:
        await env.client.start(sequence, 2, request_id="forged")
        delivery = await dispatch_first(env)
        delivery.data = b"999"
        raw = quarantine(delivery, "bad")
        with pytest.raises(ProtocolError):
            await replay_quarantine(env.client, raw, actor="operator", reason="fix", request_id="replay")


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_PULSAR"), reason="Native Pulsar not configured")
async def test_native_health_probe_does_not_consume_business_messages():
    transport = PulsarTransport(os.environ["DURAFLOW_TEST_PULSAR"])
    try:
        assert await transport.ping()
        assert not transport.consumers
    finally:
        await transport.close()


async def test_failed_dependency_disables_readiness():
    async with TestEnvironment(Registry(sequence, double)) as env:

        class Offline:
            async def ping(self):
                raise ConnectionError("private-url-and-secret")

        checks = await sample_health(env.store, Offline(), env.client.registry, "worker", timeout=0.1)
        assert checks == {"database": True, "broker": False, "registry": True}


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DURAFLOW_TEST_POSTGRES"), reason="Native PostgreSQL not configured")
async def test_native_telemetry_is_bounded_and_contains_no_payload():
    from uuid import uuid4
    from duraflow import LegacyClient as Client
    from duraflow.postgres import PostgresStore

    store = PostgresStore(os.environ["DURAFLOW_TEST_POSTGRES"])
    try:
        await store.initialize()
        namespace = "metrics-" + uuid4().hex[:12]
        client = Client(store, Registry(sequence, double), namespace=namespace)
        await client.start(sequence, 99, request_id="metrics-input")
        stats = await store.telemetry(namespace)
        assert stats["statuses"]["PENDING"] == 1
        assert stats["outbox_age"] >= 0
        assert "input" not in json.dumps(stats)
    finally:
        await store.close()
