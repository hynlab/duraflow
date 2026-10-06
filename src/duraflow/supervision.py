"""Bounded service supervision. Hard process termination belongs to the CLI/supervisor."""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Callable

from .config import RuntimeSettings
from .observability import HealthState, ProbeServer, sample_health

log = logging.getLogger("duraflow.service")


class ShutdownTimeout(RuntimeError):
    """A process supervisor must terminate the remaining execution context."""


async def supervise(runtime: Any, transport: Any, store: Any, registry: Any, settings: RuntimeSettings,
                    stop: asyncio.Event, *, role: str, on_hard_timeout: Callable[[], None] | None = None) -> None:
    health = HealthState()
    runtime.health = health
    runtime.accepting, runtime.draining = False, False
    probes = ProbeServer(health, runtime, role, max_age=settings.probe_interval + 2 * settings.probe_timeout)
    if settings.probe_port is not None:
        await probes.start(settings.probe_host, settings.probe_port)

    async def monitor() -> None:
        while not stop.is_set():
            checks = await sample_health(store, transport, registry, role, timeout=settings.probe_timeout)
            health.checks, health.checked_at = checks, time.monotonic()
            ready = all(checks.values()) and not stop.is_set()
            if ready != health.ready:
                log.info("dependency_state", extra={"role": role, "code": "READY" if ready else "NOT_READY"})
            health.ready, runtime.accepting = ready, ready
            telemetry = getattr(store, "telemetry", None)
            if telemetry is not None:
                try:
                    health.stats = await asyncio.wait_for(telemetry(settings.namespace), settings.probe_timeout)
                except Exception:
                    pass
            try:
                await asyncio.wait_for(stop.wait(), settings.probe_interval)
            except TimeoutError:
                pass

    async def hard_timeout() -> None:
        health.ready, health.draining = False, True
        runtime.accepting, runtime.draining = False, True
        log.error("shutdown_incomplete", extra={"role": role, "code": "EXTERNAL_OUTCOME_UNKNOWN"})
        if on_hard_timeout is not None:
            on_hard_timeout()
        raise ShutdownTimeout("Service failed to drain; remaining external outcomes may be unknown")

    monitor_task = asyncio.create_task(monitor())
    running = asyncio.create_task(runtime.run(stop, poll_interval=settings.poll_interval))
    requested = asyncio.create_task(stop.wait())
    log.info("runtime_started", extra={"role": role})
    try:
        done, _ = await asyncio.wait({running, requested}, return_when=asyncio.FIRST_COMPLETED)
        if running in done:
            await running
        else:
            health.ready, health.draining = False, True
            runtime.accepting, runtime.draining = False, True
            log.info("shutdown_requested", extra={"role": role})
            done, _ = await asyncio.wait({running}, timeout=settings.shutdown_timeout)
            if not done:
                running.cancel()
                await hard_timeout()
            await running
    finally:
        stop.set()
        health.alive, health.ready, health.draining = False, False, True
        runtime.accepting, runtime.draining = False, True
        requested.cancel()
        monitor_task.cancel()
        if not running.done():
            running.cancel()
        cleanup = [asyncio.create_task(runtime.close()), asyncio.create_task(transport.close()),
                   asyncio.create_task(probes.close())]
        all_tasks = {running, requested, monitor_task, *cleanup}
        done, pending = await asyncio.wait(all_tasks, timeout=min(settings.shutdown_timeout, 5))
        for task in done:
            if not task.cancelled():
                task.exception()
        if pending:
            for task in pending:
                task.cancel()
            await hard_timeout()
        log.info("runtime_stopped", extra={"role": role})
