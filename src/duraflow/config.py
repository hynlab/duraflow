"""Validated runtime limits. Secrets are excluded from representations and diagnostics."""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Any, Mapping

from .contracts import duration, name
from .security import secret_value, validate_production_connections
from .retention import RetentionPolicy


@dataclass(frozen=True)
class RuntimeSettings:
    database_url: str = field(default="sqlite:///duraflow.db", repr=False)
    broker_url: str = field(default="pulsar://localhost:6650", repr=False)
    namespace: str = "default"
    production: bool = False
    pulsar_token: str | None = field(default=None, repr=False)
    pulsar_tls_ca: str | None = field(default=None, repr=False)
    operator_role: str = "duraflow_operator"
    retention_seconds: float = 604800.0
    redelivery_safety_horizon: float = 86400.0
    concurrency: int = 8
    pool_size: int = 5
    batch_size: int = 100
    max_commands: int = 1000
    receiver_queue_size: int = 64
    max_routes: int = 256
    replay_workers: int = 2
    lease_seconds: float = 30.0
    operation_timeout: float = 15.0
    statement_timeout: float = 5.0
    lock_timeout: float = 2.0
    replay_timeout: float = 5.0
    shutdown_timeout: float = 30.0
    poll_interval: float = 0.1
    probe_interval: float = 5.0
    probe_timeout: float = 3.0
    probe_host: str = "127.0.0.1"
    probe_port: int | None = None

    def __post_init__(self) -> None:
        name(self.namespace)
        name(self.operator_role)
        if type(self.production) is not bool or len(self.operator_role) > 63:
            raise ValueError("Invalid production mode or operator role")
        for key, low, high in (
            ("concurrency", 1, 256),
            ("pool_size", 1, 100),
            ("batch_size", 1, 1000),
            ("max_commands", 1, 10000),
            ("receiver_queue_size", 1, 10000),
            ("max_routes", 1, 4096),
            ("replay_workers", 1, 32),
        ):
            value = getattr(self, key)
            if type(value) is not int or not low <= value <= high:
                raise ValueError(f"Invalid {key}")
        for key in (
            "lease_seconds",
            "operation_timeout",
            "statement_timeout",
            "lock_timeout",
            "replay_timeout",
            "shutdown_timeout",
            "poll_interval",
            "probe_interval",
            "probe_timeout",
            "retention_seconds",
            "redelivery_safety_horizon",
        ):
            value = getattr(self, key)
            if type(value) not in (int, float):
                raise ValueError(f"Invalid {key}")
            duration(value)
        RetentionPolicy(self.retention_seconds, self.redelivery_safety_horizon)
        if not self.lock_timeout < self.statement_timeout < self.operation_timeout:
            raise ValueError("Require lock_timeout < statement_timeout < operation_timeout")
        if self.lease_seconds < 3 * self.statement_timeout:
            raise ValueError("lease_seconds must allow at least three statement timeouts")
        if self.probe_port is not None and (type(self.probe_port) is not int or not 0 <= self.probe_port <= 65535):
            raise ValueError("Invalid probe_port")
        if not self.probe_host or len(self.probe_host) > 255:
            raise ValueError("Invalid probe_host")
        if not self.broker_url.startswith(("pulsar://", "pulsar+ssl://")):
            raise ValueError("Unsupported broker URL scheme")
        if self.production:
            validate_production_connections(self.database_url, self.broker_url, self.pulsar_token, self.pulsar_tls_ca)

    @classmethod
    def from_environment(cls, env: Mapping[str, str], **overrides: Any) -> RuntimeSettings:
        source = dict(env)
        for key in ("DURAFLOW_DATABASE_URL", "DURAFLOW_PULSAR_TOKEN"):
            loaded_secret = secret_value(env, key)
            if loaded_secret is not None:
                source[key] = loaded_secret
        env = source
        values: dict[str, Any] = {}
        defaults = cls()
        aliases = {"broker_url": "DURAFLOW_PULSAR_URL"}
        for item in fields(cls):
            key = aliases.get(item.name, "DURAFLOW_" + item.name.upper())
            if key not in env:
                continue
            raw = env[key]
            default = getattr(defaults, item.name)
            try:
                if item.name == "production":
                    if raw.lower() not in {"true", "false", "1", "0"}:
                        raise ValueError()
                    value: Any = raw.lower() in {"true", "1"}
                elif type(default) is int or item.name == "probe_port":
                    value = int(raw)
                elif type(default) is float:
                    value = float(raw)
                else:
                    value = raw
            except ValueError:
                raise ValueError(f"Invalid setting: {key}") from None
            values[item.name] = value
        values.update({key: value for key, value in overrides.items() if value is not None})
        return cls(**values)
