"""Shared connection factories for the Python runtime and CLI."""

from __future__ import annotations

from .config import RuntimeSettings
from .message_store import MessageStore, SQLiteMessageStore
from .security import validate_production_connections
from .transport import PulsarTransport


def open_journal(url: str, schema: str, settings: RuntimeSettings) -> MessageStore:
    if settings.production:
        validate_production_connections(url, settings.broker_url, settings.pulsar_token, settings.pulsar_tls_ca)
    if url.startswith("sqlite:///"):
        path = url.removeprefix("sqlite:///")
        if not path or "?" in path or "#" in path:
            raise ValueError("Use sqlite:///relative.db or sqlite:////absolute/path.db without URL parameters")
        return SQLiteMessageStore(path)
    if not url.startswith("postgresql+psycopg://"):
        raise ValueError("Supported database URL schemes: sqlite:/// and postgresql+psycopg://")
    from .message_postgres import PostgresMessageStore

    return PostgresMessageStore(
        url,
        schema=schema,
        pool_size=settings.pool_size,
        operation_timeout=settings.operation_timeout,
        lock_timeout=settings.lock_timeout,
        statement_timeout=settings.statement_timeout,
    )


def broker(settings: RuntimeSettings) -> PulsarTransport:
    authentication = None
    if settings.pulsar_token:
        import pulsar

        authentication = pulsar.AuthenticationToken(settings.pulsar_token)
    return PulsarTransport(
        settings.broker_url,
        authentication=authentication,
        tls_trust_certs_file_path=settings.pulsar_tls_ca,
        operation_timeout=settings.operation_timeout,
        receiver_queue_size=settings.receiver_queue_size,
        max_routes=settings.max_routes,
    )
