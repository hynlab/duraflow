# 6. Operations

[Index](0_index.md) · Previous: [Distributed services](5_distributed.md)

## Role configuration

| Role | Required connections |
| --- | --- |
| Client / broker CLI commands | Pulsar |
| `workflow-engine` | Pulsar and workflow state journal |
| `workflow-worker` | Pulsar and importable workflow code |
| `task-worker` | Pulsar and its independent execution journal |
| `tag-engine` | Pulsar and workflow-tag journal |
| `task-tag-engine` | Pulsar and task-tag journal |

The CLI ignores workflow DB credentials for broker-only commands and executors.
Python callers can do the same with `RuntimeSettings.from_environment(role=...)`.
In development, the task-worker journal defaults to `sqlite:///duraflow-tasks.db`;
production requires an explicit independently configured journal URL.

| Setting | Default / meaning |
| --- | --- |
| `DURAFLOW_DATABASE_URL` | `sqlite:///duraflow.db`; state and workflow-tag role connection |
| `DURAFLOW_TASK_JOURNAL_URL` | `sqlite:///duraflow-tasks.db` when unset in development; explicit URL required in production |
| `DURAFLOW_MESSAGE_SCHEMA` | `duraflow_messages` |
| `DURAFLOW_PULSAR_URL` | `pulsar://localhost:6650` |
| `DURAFLOW_PULSAR_TENANT` | `public` |
| `DURAFLOW_PULSAR_NAMESPACE` | `default`; physical broker environment |
| `DURAFLOW_NAMESPACE` | `default`; logical identity/routing scope |
| `DURAFLOW_CONCURRENCY` | `8`; CLI engine/task consumption concurrency |
| `DURAFLOW_REPLAY_WORKERS` | `2`; workflow replay pool |
| `DURAFLOW_MAX_COMMANDS` | `1000`; workflow history bound |
| `DURAFLOW_LEASE_SECONDS` | `30`; service execution lease |
| `DURAFLOW_OPERATION_TIMEOUT` | `15` seconds |
| `DURAFLOW_STATEMENT_TIMEOUT` | `5` seconds |
| `DURAFLOW_LOCK_TIMEOUT` | `2` seconds |
| `DURAFLOW_SHUTDOWN_TIMEOUT` | `30` seconds |
| `DURAFLOW_PROBE_PORT` | Unset; enables a dedicated probe port |

Require `lock_timeout < statement_timeout < operation_timeout`; the configured
lease allows at least three statement timeouts. Library constructors receive
configuration explicitly. See [`RuntimeSettings`](../src/duraflow/config.py).

For exact `export` commands and complete launch examples, see
[README configuration](../README.md#configure-with-environment-variables).
`RuntimeSettings(...)` uses code values and defaults only. Call
`RuntimeSettings.from_environment()` to read the current process environment,
or pass a mapping for an isolated configuration source. Explicit overrides win
over environment values; CLI flags also win over environment values. A `_FILE`
secret source is read only when that setting is not explicitly overridden.

Python fields are `database_url`, `task_journal_url`, `message_schema`,
`broker_url`, `pulsar_tenant`, `pulsar_namespace`, and `namespace` for the connection
and routing variables above. A role opens only its own required connections.
`Runtime.initialize()` initializes the selected journal before service startup;
`async with Runtime(...)` owns connections and `run(stop=event)` serves the role.

## TLS, credentials, and operator topics

Production mode requires authenticated `pulsar+ssl://` with
`DURAFLOW_PULSAR_TLS_CA` and `DURAFLOW_PULSAR_TOKEN` (or `_TOKEN_FILE`). DB-backed
roles additionally require PostgreSQL `sslmode=verify-full` and `sslrootcert`.
Workflow database secrets may use `DURAFLOW_DATABASE_URL_FILE`; task journal
secrets may use `DURAFLOW_TASK_JOURNAL_URL_FILE`.

```bash
export DURAFLOW_PRODUCTION=true
export DURAFLOW_PULSAR_URL='pulsar+ssl://broker.example.com:6651'
export DURAFLOW_PULSAR_TLS_CA='/etc/duraflow/pulsar-ca.pem'
export DURAFLOW_PULSAR_TOKEN_FILE='/run/secrets/pulsar-token'
# These files contain complete postgresql+psycopg:// URLs with
# ?sslmode=verify-full&sslrootcert=/etc/duraflow/postgres-ca.pem
export DURAFLOW_DATABASE_URL_FILE='/run/secrets/workflow-database-url'
export DURAFLOW_TASK_JOURNAL_URL_FILE='/run/secrets/task-database-url'
```

Unset corresponding direct URL/token variables before switching to `_FILE`.
Broker-only roles validate Pulsar TLS without requiring a DB URL. DB-backed roles
validate the selected journal's PostgreSQL TLS before opening it. SQLite is
supported in default local mode, but not by this strict production policy.

Secret files must be bounded regular files owned by the runtime user or root,
with permissions such as `0400` or `0640`. Configure a value or its `_FILE`
source, not both.

Use broker ACLs to distinguish application command producers, execution producers,
and operators. Workflow controls use separate `control` topics; task cancellation
and tag controls also have their own control topics. The `actor` string is audit
information, not authentication. Normal topic producers cannot bypass the control
route check by changing a message's kind.

Grant journal roles access only to their own state/inbox/outbox schema. The
older [`roles.sql`](../docs/operations/roles.sql) describes protocol-1 tables;
protocol-2 journals use the tables documented in [architecture](8_architecture.md).

## Health and metrics

Use separate probe ports for each process:

```bash
curl --fail http://127.0.0.1:8092/live
curl --fail http://127.0.0.1:8092/ready
curl --fail http://127.0.0.1:8092/metrics
```

Readiness checks the connections required by that role and pauses new admission
on failure. Executors without workflow storage do not need a workflow database
health check. Counters include consumed messages, invalid-message errors, and task
executions; labels are bounded by role.

Logs contain allowlisted structured fields. Native client logging is suppressed
so raw connection diagnostics do not leak into CLI JSON responses or structured logs.
Probe endpoints are read-only. Keep them on an operational network.

## Inspect and control

With the distributed example's settings configured:

```bash
python -m duraflow --app examples.signal_contracts describe order-42
python -m duraflow --app examples.signal_contracts history order-42
python -m duraflow --app examples.signal_contracts attempts order-42
python -m duraflow list --limit 50
```

The first three commands use broker request/response. `list` is a bounded,
read-only journal inspection command and needs DB access.

```bash
python -m duraflow --app examples.signal_contracts cancel order-42 \
  --actor operator --reason 'Customer request' --request-id cancel-42 --yes
```

Cancel ends orchestration and requests cooperative task cancellation. Terminate
also fences pending workflow execution. Neither reverses external effects or
proves a running Python thread stopped. A stuck replay executor can be fenced
through the control topic even while its activation is in flight.

For exhausted tasks, inspect the node ID and use:

```bash
python -m duraflow --app examples.signal_contracts retry order-42 '1.0' \
  --actor operator --reason 'Dependency repaired' --request-id retry-42 --yes
```

`resume` applies to blocked workflows without exhausted task nodes. Terminal runs
cannot be reopened. Controls record audit events and deduplicate request IDs.

## Recovery and retention

State engines commit input identity, state changes, and outgoing intent atomically.
Outbox relays recover uncompleted sends. Task workers persist their fenced results
in an independent journal before ACK. A replacement can reuse a result committed
before the old worker's ACK, or reclaim an expired unfinished attempt.

Timers and retries are broker-delayed messages. Consumers validate `not_before`
against journal time; early deliveries are NACKed for later delivery.

Configure broker retention and TTL to cover worker outages and the longest delayed
operation. Internal envelopes support up to 16 MiB; configure the broker's message
limit accordingly if using large workflow histories. Keep application payloads
small and use bounded children or rollover for long histories.

Archive only settled terminal histories after the configured retention period:

```bash
python -m duraflow --app examples.signal_contracts archive order-42 \
  --retention 604800 --safety-horizon 86400 \
  --actor operator --reason 'Retention elapsed' --yes
```

The production engine enforces `DURAFLOW_RETENTION_SECONDS` and
`DURAFLOW_REDELIVERY_SAFETY_HORIZON` floors. Archive removes payload/history detail
while retaining start and signal identity tombstones. Task journals and tag indexes
have independent retention obligations; do not delete them while retained broker
deliveries could reuse their execution identities.

Invalid protocol-2 deliveries go to `{source_topic}-dlq`. Inspect them with broker
tools and reconcile the original request before republishing. The legacy
`dlq-peek` / `dlq-replay` envelope commands apply only to protocol 1 via `--legacy`.

## Upgrading from protocol 1

Protocol 2 uses separate `df2-` topics and message-journal tables. It changes signal
semantics to explicit receive registration. Existing protocol-1 histories retain
their former pre-wait buffering semantics.

1. Drain earlier runs with `LegacyClient`, `LegacyEngine`, `LegacyWorker`, or
   `python -m duraflow --legacy ...` using their existing database.
2. Initialize protocol-2 workflow and task journals in separate schemas.
3. Provision and start the new runtime roles.
4. Route new starts to the message-driven client and `df2-` topics.
5. Retain earlier journals until their delivery and retention obligations expire.

Running protocol-1 histories are not silently converted. The **execution/message
protocol is 2**. SQLite message-journal schema version is 1; PostgreSQL message-journal
schema version is 2. These are independent version numbers.

### PostgreSQL message-journal schema 2

Stop DB-backed roles and run the normal journal initialization for each workflow,
task, and tag schema before restarting them with the updated package. Initialization
upgrades schema 1 transactionally under an advisory lock; concurrent initializers
serialize and a failed upgrade rolls back.

Schema 2 uses `JSON` instead of `JSONB` for state/outbox documents. JSONB normalizes
values such as `1e20` and `-0.0`, which can change replay fingerprints and publication
identities. JSON retains the representation required by the wire codec. Existing
documents, inbox identities, outbox owners, and leases are preserved during upgrade;
numeric representations already normalized by an older journal cannot be recovered
from that journal alone. Unknown/newer schema versions are rejected.
