"""Reviewed D01-D03 changes; applied once to the verified phase-three tree."""
from pathlib import Path
from helpers import add_method, function, replace, write

replace('src/duraflow/config.py', 'from .contracts import duration, name', 'from .contracts import duration, name\nfrom .security import secret_value, validate_production_connections\nfrom .retention import RetentionPolicy')
replace('src/duraflow/config.py', '    production: bool = False', '''    production: bool = False
    pulsar_token: str | None = field(default=None, repr=False)
    pulsar_tls_ca: str | None = field(default=None, repr=False)
    operator_role: str = "duraflow_operator"
    retention_seconds: float = 604800.0
    redelivery_safety_horizon: float = 86400.0''')
function('src/duraflow/config.py', 'RuntimeSettings.__post_init__', '''
def __post_init__(self) -> None:
    name(self.namespace)
    name(self.operator_role)
    if type(self.production) is not bool or len(self.operator_role) > 63:
        raise ValueError("Invalid production mode or operator role")
    for key, low, high in (
        ("concurrency", 1, 256), ("pool_size", 1, 100), ("batch_size", 1, 1000),
        ("max_commands", 1, 10000), ("receiver_queue_size", 1, 10000),
        ("max_routes", 1, 4096), ("replay_workers", 1, 32),
    ):
        value = getattr(self, key)
        if type(value) is not int or not low <= value <= high:
            raise ValueError(f"Invalid {key}")
    for key in ("lease_seconds", "operation_timeout", "statement_timeout", "lock_timeout",
                "replay_timeout", "shutdown_timeout", "poll_interval", "probe_interval", "probe_timeout",
                "retention_seconds", "redelivery_safety_horizon"):
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
''')
replace('src/duraflow/config.py', '        values: dict[str, Any] = {}', '''        source = dict(env)
        for key in ("DURAFLOW_DATABASE_URL", "DURAFLOW_PULSAR_TOKEN"):
            value = secret_value(env, key)
            if value is not None:
                source[key] = value
        env = source
        values: dict[str, Any] = {}''')
replace('src/duraflow/transport.py', '"connection_timeout_ms": 5000}', '"connection_timeout_ms": 5000, "tls_allow_insecure_connection": False, "tls_validate_hostname": True}')
replace('src/duraflow/postgres.py', '        statement_timeout: float = 5.0,\n    ):', '''        statement_timeout: float = 5.0,
        control_role: str | None = None,
        retention_policy: Any = None,
    ):''')
replace('src/duraflow/postgres.py', '        name(schema)', '''        from .retention import RetentionPolicy
        if control_role is not None:
            name(control_role)
            if len(control_role) > 63:
                raise ValueError("Operator role exceeds PostgreSQL identifier bound")
        self.control_role = control_role
        self.retention_policy = retention_policy or RetentionPolicy()
        name(schema)''')
add_method('src/duraflow/postgres.py', 'PostgresStore', '''
async def principal(self) -> str:
    from sqlalchemy import text
    async with self._transaction() as conn:
        return str((await conn.execute(text("SELECT current_user"))).scalar_one())
''')
add_method('src/duraflow/postgres.py', 'PostgresStore', '''
async def authorize_control(self, action: str) -> str:
    from sqlalchemy import text
    from .security import AuthorizationError, CONTROLS
    if action not in CONTROLS:
        raise AuthorizationError("Unknown administrative operation")
    async with self._transaction() as conn:
        if self.control_role is None:
            return str((await conn.execute(text("SELECT current_user"))).scalar_one())
        row = (await conn.execute(text("SELECT current_user AS principal, EXISTS ("
            "SELECT 1 FROM pg_roles WHERE rolname = :role AND pg_has_role(current_user, oid, 'USAGE')"
            ") AS allowed"), {"role": self.control_role})).mappings().one()
        if not row["allowed"]:
            raise AuthorizationError("Database principal lacks the required operator role")
        return str(row["principal"])
''')
p = Path('src/duraflow/client.py')
text = p.read_text()
text = text.replace('from .client import', 'from .client import')
text = text.replace('from __future__ import annotations', 'from __future__ import annotations\n\nfrom .security import authorize_control\nfrom .retention import RetentionPolicy\nfrom .contracts import clock_now')
text = text.replace('self.client.clock.now()', 'clock_now(self.client.clock)').replace('self.clock.now()', 'clock_now(self.clock)')
p.write_text(text)
replace('src/duraflow/client.py', 'self, action: str, *, actor: str, reason: str, request_id: str, node_id: str | None = None', 'self, action: str, *, actor: str, reason: str, request_id: str, node_id: str | None = None, _internal: bool = False')
replace('src/duraflow/client.py', '        digest = fingerprint([action, actor, reason, node_id])', '        principal = await authorize_control(self.client.store, action, internal=_internal)\n        digest = fingerprint([action, actor, reason, node_id])')
replace('src/duraflow/client.py', 'event(state, "operator_action", now, action=action, actor=actor, reason=reason, node_id=node_id)', 'event(state, "operator_action", now, action=action, actor=actor, reason=reason, node_id=node_id, principal=principal)')
replace('src/duraflow/client.py', '        duration(safety_horizon)', '''        principal = await authorize_control(self.client.store, "archive")
        policy = getattr(self.client.store, "retention_policy", RetentionPolicy())
        policy.validate(retention, safety_horizon)
        if len(actor) > 128 or len(reason) > 1000:
            raise ValueError("Archive audit fields exceed limits")
        duration(safety_horizon)''')
replace('src/duraflow/client.py', '            state["input"], state["result"] = None, None', '            state["input"], state["result"], state["error"], state["blocked_reason"] = None, None, None, None')
replace('src/duraflow/client.py', 'event(state, "archived", clock_now(self.client.clock), actor=actor, reason=reason)', 'event(state, "archived", clock_now(self.client.clock), actor=actor, reason=reason, principal=principal)')
replace('src/duraflow/coordinator.py', 'self.client.get_handle(child_id).cancel(', 'self.client.get_handle(child_id)._control("cancel", _internal=True,')
replace('src/duraflow/cli.py', 'from .config import RuntimeSettings', 'from .config import RuntimeSettings\nfrom .retention import RetentionPolicy')
replace('src/duraflow/cli.py', 'root.add_argument("--database", default=os.environ.get("DURAFLOW_DATABASE_URL", "sqlite:///duraflow.db"))', 'root.add_argument("--database", default=None)')
function('src/duraflow/cli.py', 'open_store', '''
async def open_store(url: str, settings: RuntimeSettings | None = None) -> Store:
    settings = settings or RuntimeSettings(database_url=url)
    if url.startswith("sqlite:///"):
        return SQLiteStore(url.removeprefix("sqlite:///"))
    from .postgres import PostgresStore
    return PostgresStore(url, pool_size=settings.pool_size, operation_timeout=settings.operation_timeout,
        lock_timeout=settings.lock_timeout, statement_timeout=settings.statement_timeout,
        control_role=settings.operator_role if settings.production else None,
        retention_policy=RetentionPolicy(settings.retention_seconds, settings.redelivery_safety_horizon)
                         if settings.production else None)
''')
p = Path('src/duraflow/cli.py')
text = p.read_text().replace('os.environ.get("DURAFLOW_PULSAR_TOKEN")', 'settings.pulsar_token').replace('os.environ.get("DURAFLOW_PULSAR_TLS_CA")', 'settings.pulsar_tls_ca')
p.write_text(text)
function('src/duraflow/cli.py', 'main', '''
def main() -> None:
    try:
        print(json.dumps(asyncio.run(execute(parser().parse_args())), indent=2, ensure_ascii=False))
    except Exception as exc:
        # Driver exception text can contain URLs, SQL parameters and secrets.
        print(f"{type(exc).__name__}: command failed; check configuration or authorized run diagnostics", file=sys.stderr)
        raise SystemExit(2) from None
''')
replace('src/duraflow/cli.py', 'DuraflowError, Registry', 'Registry')
write('docs/hardening/phase4.md', '''
# Phase 4 — connection policy, controls and retention

D01: Production settings require PostgreSQL sslmode=verify-full with an explicit
readable CA and a hostname, and authenticated pulsar+ssl with an explicit CA.
The official Pulsar adapter disables insecure certificates and validates hostname.
DATABASE_URL and PULSAR_TOKEN accept exactly one direct or _FILE source. File
reads are bounded, regular-file-only and reject symlinks, world access, writable
by others and unexpected owners; provision container secrets with mode 0400/0640.
Mounted Kubernetes symlink projections require a reviewed materialization step;
this implementation deliberately does not silently follow arbitrary symlinks.
CLI errors never print raw connection or driver exception text. Configure broker
server-side authorization and private network access independently.

D02: --actor is audit context, NOT authenticated identity. PostgreSQL checks
current_user and actual membership of the configured operator role for controls,
archive and DLQ replay, and records that principal. The SQL deployment template
separates NOLOGIN reader, runtime writer, operator and migration groups. Actual
login accounts and secrets must be provisioned by the operator, not committed.
Readers cannot write even if they bypass the SDK. Runtime writers remain trusted:
they hold SQL UPDATE and can deliberately bypass Python checks. This is NOT
hostile-worker or multi-tenant isolation. Parent cleanup uses an explicit private
runtime path rather than trusting an actor string or granting every worker the
operator role. The schema owner/migration account is not a runtime credential.

D03: Production archive requests must satisfy deployment retention and redelivery
horizon floors as well as the existing terminal/no-pending-work/no-unsent-message
checks. Full payloads and errors are removed only after safe archival; request,
workflow-head, signal-key and action tombstones remain. No automatic destructive
GC, live-history truncation or tombstone expiry is introduced. Backups, broker
retention, external idempotency and audit-data privacy need matching policies.

Tests exercise fail-closed settings, unsafe secret sources, explicit TLS flags,
principal spoofing rejection, retained tombstones and native PostgreSQL reader
and operator roles. These checks do not certify the user's actual certificate
chain, broker ACL configuration or backup/PITR deployment.
''')
