# Phase 5 — native failure qualification

The qualification suite uses real PostgreSQL and Pulsar with two separate engine
processes, isolated replay subprocesses, and three separately killable workers.
A test-only SQLite ledger models an independent durable external service with a
stable idempotency key. It is intentionally outside the engine database: engine
recovery cannot erase evidence of already performed external work.

Deterministic file barriers identify before-side-effect, after-external-commit,
and after-result-record/before-ACK boundaries. SIGKILL is real. Recovered tasks
may be invoked again across an uncertain side-effect boundary, but the external
ledger accepts one effect per stable key; committed results must not be rerun.
Tests include two-engine recovery after partial fan-in, actual broker SIGKILL,
and actual PostgreSQL SIGKILL with real lease expiry and reconnect. They do not
assert arbitrary external exactly-once execution.

Destructive tests are excluded from make native-check. Run make native-fault-check
only with DURAFLOW_ALLOW_DESTRUCTIVE_TESTS=isolated-compose and an explicitly
isolated COMPOSE_PROJECT_NAME=duraflow-qualification-<suffix>. The guard validates
local service endpoints and Docker Compose project/service ownership before any
kill. A pre-existing production project is rejected. Do not run these tests
concurrently or on a machine where the configured local ports belong to other
services. Credentials/logs are confined to disposable qualification infrastructure.

Current source supplies E01/E02/E03/E04 and multi-engine portions of E06. E05
physical backup/WAL point-in-time recovery is a separate required qualification
case and is not yet claimed passed. Evidence is emitted to qualification-results.json
only after case assertions succeed. CI job success, exact source and remaining
release gates must be recorded separately; a test's existence is not a pass.
