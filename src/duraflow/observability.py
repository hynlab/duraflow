"""Payload-free operational logs, bounded metric labels and read-only probes."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any

EVENTS = frozenset(
    {
        "runtime_message",
        "engine_iteration_failed",
        "worker_iteration_failed",
        "task_claimed",
        "task_observed",
        "workflow_state_saved",
        "shutdown_requested",
        "shutdown_incomplete",
        "runtime_started",
        "runtime_stopped",
        "dependency_state",
        "message_quarantined",
        "dlq_requeued",
    }
)
SAFE_FIELDS = frozenset({"run_id", "task_id", "node_id", "attempt", "epoch", "event_id", "error_type", "code", "role"})
COUNTERS = frozenset(
    {
        "activations",
        "cas_conflicts",
        "publications",
        "transport_errors",
        "unsupported_activations",
        "executed",
        "duplicates",
        "stale_results",
        "quarantined",
    }
)
STATUSES = (
    "PENDING",
    "WAITING",
    "BLOCKED",
    "CANCELLING",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TERMINATED",
    "CONTINUED",
)


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = str(record.msg)
        if event not in EVENTS:
            event = "runtime_message"
        data: dict[str, Any] = {"time": record.created, "level": record.levelname, "event": event}
        for key in SAFE_FIELDS:
            value: object = getattr(record, key, None)
            if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:/-]{1,128}", value):
                data[key] = value
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                try:
                    if math.isfinite(float(value)):
                        data[key] = value
                except OverflowError:
                    pass
        if record.exc_info and record.exc_info[0]:
            data["error_type"] = record.exc_info[0].__name__
        return json.dumps(data, separators=(",", ":"), allow_nan=False)


def configure_logging() -> None:
    logger = logging.getLogger("duraflow")
    if not any(getattr(handler, "_duraflow", False) for handler in logger.handlers):
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(JsonLogFormatter())
        setattr(handler, "_duraflow", True)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


@dataclass
class HealthState:
    alive: bool = True
    ready: bool = False
    draining: bool = False
    checks: dict[str, bool] = field(default_factory=dict)
    checked_at: float = 0
    started_at: float = field(default_factory=time.monotonic)
    stats: dict[str, Any] = field(default_factory=dict)

    def readiness(self, max_age: float) -> bool:
        return self.alive and self.ready and not self.draining and time.monotonic() - self.checked_at <= max_age


def render_metrics(runtime: Any, health: HealthState, role: str) -> str:
    if role not in {"engine", "worker"}:
        raise ValueError("Unknown runtime role")
    lines = []
    for key in sorted(COUNTERS):
        if key in runtime.metrics:
            lines.append(f'duraflow_{key}_total{{role="{role}"}} {int(runtime.metrics[key])}')
    lines.extend(
        [
            f'duraflow_ready{{role="{role}"}} {int(health.ready and not health.draining)}',
            f'duraflow_draining{{role="{role}"}} {int(health.draining)}',
            f'duraflow_uptime_seconds{{role="{role}"}} {max(0, time.monotonic() - health.started_at):.3f}',
            f'duraflow_due_work_lag_seconds{{role="{role}"}} {max(0, float(health.stats.get("due_lag", 0))):.3f}',
        ]
    )
    lines.append(
        f'duraflow_sampled_outbox_age_seconds{{role="{role}"}} {max(0, float(health.stats.get("outbox_age", 0))):.3f}'
    )
    counts = health.stats.get("statuses", {})
    for status in STATUSES:
        lines.append(f'duraflow_runs{{role="{role}",status="{status}"}} {int(counts.get(status, 0))}')
    return "\n".join(lines) + "\n"


async def sample_health(store: Any, transport: Any, registry: Any, role: str, *, timeout: float) -> dict[str, bool]:
    async def database() -> bool:
        version = getattr(store, "schema_version", None)
        if version is not None:
            return await version() == 2
        await store.scan("default", limit=1)
        return True

    async def broker() -> bool:
        ping = getattr(transport, "ping", None)
        return True if ping is None else bool(await ping())

    async def guarded(fn: Any) -> bool:
        try:
            return bool(await asyncio.wait_for(fn(), timeout))
        except Exception:
            return False

    db, messaging = await asyncio.gather(guarded(database), guarded(broker))
    registered = bool(registry.workflows if role == "engine" else registry.tasks)
    return {"database": db, "broker": messaging, "registry": registered}


class ProbeServer:
    """Small HTTP/1.0 GET surface; no workflow data and no state-changing endpoints."""

    def __init__(self, health: HealthState, runtime: Any, role: str, *, max_age: float = 15.0):
        self.health, self.runtime, self.role, self.max_age = health, runtime, role, max_age
        self.server: asyncio.Server | None = None
        self.active = 0

    async def start(self, host: str, port: int) -> int:
        self.server = await asyncio.start_server(self.handle, host, port, limit=8192)
        assert self.server.sockets is not None
        return int(self.server.sockets[0].getsockname()[1])

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        if self.active >= 16:
            writer.close()
            return
        self.active += 1
        try:
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 1.0)
            if len(request) > 4096:
                return
            parts = request.split(b"\r\n", 1)[0].split(b" ")
            if len(parts) != 3 or parts[0] != b"GET":
                status, body, content_type = 405, b"{}", "application/json"
            elif parts[1] == b"/live":
                status = 200 if self.health.alive else 503
                body, content_type = json.dumps({"alive": self.health.alive}).encode(), "application/json"
            elif parts[1] == b"/ready":
                ready = self.health.readiness(self.max_age)
                status = 200 if ready else 503
                body = json.dumps(
                    {"ready": ready, "draining": self.health.draining, "checks": self.health.checks}
                ).encode()
                content_type = "application/json"
            elif parts[1] == b"/metrics":
                status, body = 200, render_metrics(self.runtime, self.health, self.role).encode()
                content_type = "text/plain; version=0.0.4"
            else:
                status, body, content_type = 404, b"{}", "application/json"
            writer.write(
                f"HTTP/1.0 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await asyncio.wait_for(writer.drain(), 1.0)
        except (TimeoutError, ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            self.active -= 1
            writer.close()

    async def close(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
