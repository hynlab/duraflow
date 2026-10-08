"""Trusted deployment security policy; database grants remain the hard boundary."""

from __future__ import annotations

import os
import ssl
import stat
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

from .contracts import DuraflowError


class AuthorizationError(DuraflowError):
    """The actual database principal is not authorized for this control."""


CONTROLS = frozenset({"cancel", "terminate", "resume", "retry", "archive", "dlq-replay"})


def secret_value(env: Mapping[str, str], key: str) -> str | None:
    """Read one explicit source, with a bounded, non-following regular-file read."""
    if key in env and key + "_FILE" in env:
        raise ValueError(f"Configure only one source for {key}")
    if key in env:
        value = env[key]
    elif key + "_FILE" in env:
        descriptor = None
        try:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0) | os.O_NONBLOCK
            descriptor = os.open(env[key + "_FILE"], flags)
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o027 or info.st_size > 16384:
                raise ValueError()
            if hasattr(os, "geteuid") and info.st_uid not in {0, os.geteuid()}:
                raise ValueError()
            value = os.read(descriptor, 16385).decode("utf-8").rstrip("\r\n")
        except (OSError, UnicodeError, ValueError):
            raise ValueError(f"Unreadable or unsafe secret source for {key}") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
    else:
        return None
    if not isinstance(value, str) or not value or len(value.encode()) > 16384 or any(ord(c) < 32 for c in value):
        raise ValueError(f"Invalid secret value for {key}")
    return value


def validate_ca(path: str | None, label: str) -> None:
    try:
        if not path or not stat.S_ISREG(os.stat(path).st_mode) or os.stat(path).st_mode & 0o022:
            raise ValueError()
        ssl.create_default_context(cafile=path)
    except (OSError, ssl.SSLError, ValueError):
        raise ValueError(f"A readable, non-writable-by-others CA bundle is required for {label}") from None


def validate_production_connections(database_url: str, broker_url: str, token: str | None, ca: str | None) -> None:
    """Fail closed before any connection or secret-bearing authentication request."""
    try:
        db = urlsplit(database_url)
        params = parse_qs(db.query, keep_blank_values=True, strict_parsing=True)
        if db.scheme != "postgresql+psycopg" or not db.hostname or not db.username or not db.path.strip("/"):
            raise ValueError()
        if params.get("sslmode") != ["verify-full"] or len(params.get("sslrootcert", [])) != 1:
            raise ValueError()
        if any(len(values) != 1 for values in params.values()) or any(
            key in params for key in ("service", "host", "hostaddr")
        ):
            raise ValueError()
        if db.fragment:
            raise ValueError()
        _ = db.port
    except (TypeError, ValueError):
        raise ValueError(
            "Production requires verified PostgreSQL TLS, authenticated Pulsar TLS and explicit CA bundles"
        ) from None
    validate_ca(params["sslrootcert"][0], "PostgreSQL")
    validate_production_broker(broker_url, token, ca)


def validate_production_broker(broker_url: str, token: str | None, ca: str | None) -> None:
    """Executors and clients validate TLS without needing workflow DB credentials."""
    try:
        broker = urlsplit(broker_url)
        if (
            broker.scheme != "pulsar+ssl"
            or not broker.hostname
            or broker.username
            or broker.password
            or broker.query
            or broker.fragment
            or not token
            or len(token.encode()) > 16384
            or any(ord(c) < 32 for c in token)
        ):
            raise ValueError()
        _ = broker.port
    except (TypeError, ValueError):
        raise ValueError("Production requires authenticated Pulsar TLS and an explicit CA bundle") from None
    validate_ca(ca, "Pulsar")


async def authorize_control(store: Any, action: str, *, internal: bool = False) -> str:
    if action not in CONTROLS:
        raise AuthorizationError("Unsupported control")
    if internal:
        # Internal parent cleanup runs as a trusted runtime writer. This private
        # path is not a security boundary against a principal already holding SQL UPDATE.
        principal = getattr(store, "principal", None)
        return "runtime:" + (str(await principal()) if principal is not None else "trusted-local")
    hook = getattr(store, "authorize_control", None)
    return str(await hook(action)) if hook is not None else "trusted-local"
