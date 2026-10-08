# Operations

[Guide index](README.md) · Previous: [Running distributed services](distributed.md)

## Runtime configuration

The CLI reads `DURAFLOW_*` environment variables through `RuntimeSettings`.
Explicit CLI options override their corresponding environment settings.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DURAFLOW_DATABASE_URL` | `sqlite:///duraflow.db` | Workflow storage |
| `DURAFLOW_PULSAR_URL` | `pulsar://localhost:6650` | Broker address |
| `DURAFLOW_NAMESPACE` | `default` | Identity and routing scope |
| `DURAFLOW_CONCURRENCY` | `8` | Concurrent worker tasks |
| `DURAFLOW_POOL_SIZE` | `5` | PostgreSQL connection pool size |
| `DURAFLOW_BATCH_SIZE` | `100` | Engine reconciliation page size |
| `DURAFLOW_MAX_COMMANDS` | `1000` | Workflow command limit |
| `DURAFLOW_REPLAY_WORKERS` | `2` | Replay subprocess pool size |
| `DURAFLOW_REPLAY_TIMEOUT` | `5` | Replay activation timeout, seconds |
| `DURAFLOW_LEASE_SECONDS` | `30` | Worker lease duration |
| `DURAFLOW_OPERATION_TIMEOUT` | `15` | Native operation timeout, seconds |
| `DURAFLOW_STATEMENT_TIMEOUT` | `5` | PostgreSQL statement timeout, seconds |
| `DURAFLOW_LOCK_TIMEOUT` | `2` | PostgreSQL lock timeout, seconds |
| `DURAFLOW_SHUTDOWN_TIMEOUT` | `30` | Service shutdown budget, seconds |
| `DURAFLOW_PROBE_HOST` | `127.0.0.1` | Probe bind address |
| `DURAFLOW_PROBE_PORT` | unset | Enable probes on a dedicated port |

Configuration requires `lock_timeout < statement_timeout < operation_timeout`
and a lease of at least three statement timeouts. See
[`RuntimeSettings`](../src/duraflow/config.py) for all limits.

When using library APIs directly, configure stores, transports, and runtimes
explicitly; constructing a `Client` does not automatically apply CLI settings.

## Connections and credentials

Production CLI mode (`--production` or `DURAFLOW_PRODUCTION=true`) requires:

- PostgreSQL with `postgresql+psycopg://`, `sslmode=verify-full`, and an explicit
  `sslrootcert` CA file.
- Pulsar with a `pulsar+ssl://` address, `DURAFLOW_PULSAR_TLS_CA`, and a token.

For file-mounted credentials, set `DURAFLOW_DATABASE_URL_FILE` and
`DURAFLOW_PULSAR_TOKEN_FILE`. Configure either a value or its `_FILE` source, not
both. Secret files must be bounded regular files owned by the runtime user or
root; permissions such as `0400` or `0640` are supported.

The PostgreSQL templates in [`docs/operations/roles.sql`](../docs/operations/roles.sql)
describe runtime, inspection, and operator grants. Control authorization checks
the database principal; `--actor` is audit information, not authentication.
Namespaces organize workloads but are not a hostile-tenant security boundary.

## Health and metrics

Enable a distinct probe port for each process, as in the distributed guide:

```bash
curl --fail http://127.0.0.1:8092/live
curl --fail http://127.0.0.1:8092/ready
curl --fail http://127.0.0.1:8092/metrics
```

- `/live` reports process liveness.
- `/ready` checks current dependency readiness and draining state.
- `/metrics` exposes Prometheus-format metrics with bounded labels.

Readiness failure stops admission of new work. Service logs use structured JSON
with allowlisted fields. Probe endpoints are read-only and do not consume
business messages. Keep them on your operational network.

An example alert configuration is available in
[`docs/operations/alerts.yml`](../docs/operations/alerts.yml).

## Inspect and control runs

With connection and namespace settings configured:

```bash
python -m duraflow list --limit 50
python -m duraflow describe "$RUN_ID"
python -m duraflow history "$RUN_ID" --after 0 --limit 100
python -m duraflow attempts "$RUN_ID"
```

For a run that needs cooperative cancellation:

```bash
python -m duraflow cancel "$RUN_ID" \
  --actor operator --reason 'Requested by customer' \
  --request-id cancel-42 --yes
```

`terminate` ends orchestration immediately; it does not prove that every external
effect has stopped. `resume` resumes a blocked execution without an exhausted
task. For a blocked task, inspect its node ID and use:

```bash
python -m duraflow retry "$RUN_ID" '0.0' \
  --actor operator --reason 'Dependency restored' \
  --request-id retry-42 --yes
```

These commands record an audit event. Use stable request IDs when retrying the
same control request. Terminal executions cannot be reopened; a new business
execution needs a new identity.

## Recovery and upgrades

PostgreSQL stores a JSON aggregate for each run, with transactional mutations,
revision checks, and indexed due-work projections. Pulsar transports deliveries;
the engine reconciles pending dispatch from the stored state.

After a worker crash, an expired lease can be reclaimed. Results committed before
an ACK are reused. A crash after an external effect but before result persistence
can invoke the task again with the same idempotency key.

The CLI uses a bounded replay subprocess pool. Workflow code is trusted code,
not a security sandbox. Supervise engine and worker processes and allow their
shutdown budget before forcing termination.

For a storage upgrade, stop old runtime writers, back up the database, and run
`python -m duraflow migrate` with the intended database settings. The current
adapter supports migration from schema 1 to 2 and refuses unknown versions or
downgrades. This is a maintenance upgrade, not an online mixed-runtime migration.

Restore testing must account for three independent histories: PostgreSQL,
Pulsar, and external business systems. Restoring the database does not roll back
the other two. Choose retention and reconciliation procedures together.

## Retention and dead letters

Archive only completed terminal histories whose pending delivery and task
obligations are settled:

```bash
python -m duraflow archive "$RUN_ID" \
  --retention 604800 --safety-horizon 86400 \
  --actor operator --reason 'Retention period elapsed' --yes
```

Values are seconds. Production settings enforce configured retention floors
(`DURAFLOW_RETENTION_SECONDS` and `DURAFLOW_REDELIVERY_SAFETY_HORIZON`). Archiving
removes payload/history detail but keeps identity tombstones for deduplication.

Malformed or conflicting deliveries are quarantined. `dlq-peek --topic TOPIC`
inspects envelopes; `dlq-replay --file ENVELOPE.json` requires actor, reason,
request ID, and `--yes`, and validates the envelope against committed work before
requeueing. Completed or superseded tasks are not reopened.

See [Testing and contributing](testing.md) for the isolated fault/recovery suite.
