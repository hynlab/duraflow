# Deployment, inspection and recovery

> Historical protocol-1 operations. For the current message-driven roles and
> commands, see [guide 6](../guide/6_operations.md). Add `--legacy` to the commands
> below when draining existing protocol-1 runs.

## Deployment assumptions

Run PostgreSQL 16 and a supported Pulsar broker independently of application
processes. The repository compose file is only a local developer environment.
A single database or broker is not made highly available by this library.

Use one engine process initially, plus independently deployed workers. Multiple
engines use CAS revisions and must have compatible pinned workflow manifests.
Old implementations must remain registered while old runs exist. Supply explicit
immutable build IDs covering helper modules and package dependencies; the fallback
function-source hash cannot account for changes in imported code.

Use narrow DB roles and broker topic permissions, TLS and network restrictions.
The SDK/engine/runner are trusted components with shared-store access; namespaces
are routing partitions, not an authorization boundary. Do not expose the DB or
CLI to untrusted tenants. Never import task code based on incoming message names.

For authenticated brokers, supply DURAFLOW_PULSAR_TOKEN and optionally
DURAFLOW_PULSAR_TLS_CA through a secret manager. Avoid credentials in shell history
or source control. Give DLQ readers only the access required: poison-message DLQs
retain the original payload, even though default logs do not print it.

## Commands

```bash
python -m duraflow --app examples.application init
python -m duraflow health
python -m duraflow list --limit 100
python -m duraflow describe RUN_ID
python -m duraflow history RUN_ID --after 0 --limit 100
python -m duraflow attempts RUN_ID
python -m duraflow cancel RUN_ID --actor operator --reason 'user request' --request-id cancel-1 --yes
python -m duraflow terminate RUN_ID --actor operator --reason 'stop orchestration' --request-id stop-1 --yes
python -m duraflow retry RUN_ID 0.0 --actor operator --reason 'dependency fixed' --request-id retry-1 --yes
python -m duraflow resume RUN_ID --actor operator --reason 'old implementation restored' --request-id resume-1 --yes
```

Global options such as --database, --namespace and --app precede the subcommand.
`describe` excludes inputs/results unless --include-payload is given. Attempts and
full payload inspection are privileged operations. `health` currently tests store
readability, not broker readiness or full application readiness.

## Recovery procedure

1. Preserve the DB, broker subscription cursors, pinned application artifacts and
   credentials. Do not delete task records or histories to make a run start again.
2. Restart the engine with the original workflow manifests. Missing/mismatching
   versions block explicitly. Restore the correct implementation before audited
   resume; do not change a build ID to hide different code.
3. Restart workers with the declared task contracts/subscriptions. Live leases
   protect current owners; expired leases can be reacquired with a new epoch.
4. Inspect task attempts, pending outbox records and blocked reasons. Repeated
   delivery and send-before-delivery-mark crashes are expected and deduplicated.
5. Resolve failed dependencies and retry only explicitly blocked tasks. Cancel and
   terminate are not side-effect rollback.

The subprocess recovery test kills the first process after two broadcast handlers
finish, backs up/restores a file DB, then starts fresh runtime objects. It verifies
only the unfinished handler executes. This is evidence for the development adapter,
not equivalent evidence for production PostgreSQL/Pulsar crash behavior.

## Backup/restore and retention

Back up PostgreSQL using your normal transactional backup/PITR procedures and
verify restores in a separate environment. Broker retention, DB retention and
operator retry windows must be defined together. Restoring a DB to before an
externally completed effect can cause that effect again; idempotency keys and
business reconciliation are required. Do not publish a blanket exactly-once claim.

Active histories cannot be purged. `archive` requires a terminal run, no unfinished
operations/publications, and elapsed retention at least as long as the explicitly
provided maximum redelivery/restore safety horizon. It keeps request/head tombstones
so a repeated old start does not become a new execution. The operator is responsible
for selecting a real safety horizon; the engine cannot infer broker retention.
No automatic purge or automatic deletion of deployment artifacts is enabled.

## Limits and shutdown

Inline data is limited to 256 KiB; keep larger artifacts in application-managed
storage and pass references. Initial defaults bound worker concurrency, broker
receive queues, routes, group size and commands per run. Use continue_as_new or
bounded child histories before the command limit. Aggregate writes and polling
must be benchmarked for your workload before increasing limits.

Engine/Worker expose counters and persisted diagnostic records. A complete
Prometheus/OpenTelemetry exporter, storage lag dashboards, 24-hour soak tests and
throughput/memory benchmarks are **not yet qualified**. No fixed performance
numbers are advertised.

SIGTERM stops acquisition and allows a bounded async drain. An uncooperative sync
Python task cannot be killed safely by its wrapper; process supervisors must own
hard-kill policy. External work may outlive orchestration termination. Apply stable
TaskContext.idempotency_key values to idempotent downstream operations.
