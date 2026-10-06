# Implementation and release status

Version: **0.1.0a1 — alpha**. The planning baseline is not a claim that every
production qualification gate has passed. No version 1.0, PyPI publication,
JVM interoperability or external exactly-once guarantee is asserted.

## Implemented

| Area | Concrete implementation |
|---|---|
| Python API | Preserving decorators, generic refs, dataclass/JSON validation, narrow protocols |
| Workflow runtime | Coroutine replay, pinned manifest, command comparison, recorded errors/time/UUID |
| Coordination | Sequential calls, all-success broadcast, ordered gather, committed-winner race |
| Durability | Durable starts, request tombstones, CAS, inbox/outbox, fenced leases, stale-result audit |
| Lifecycle | Retry policies, schedule/attempt/overall deadlines, durable timers, signals, cancellation |
| Operations | History/status/attempts, audited controls, retry blocked tasks, tagged page controls |
| Composition | Child workflows, rollover/logical heads, delegated typed completion tokens |
| Storage | Memory, file-backed SQLite and optional SQLAlchemy 2/psycopg PostgreSQL |
| Transport | Memory and optional official Pulsar client, scoped subscriptions, bounded queues |
| Delivery | CLI, installable wheel/sdist, typed marker, examples, local compose, CI |

## Local evidence

The authored test suite was executed on Python 3.13.5 with Pydantic 2.13.4:
**57 tests passed; 3 skips** (two native-service tests and the optional Hypothesis
module, whose runtime was not installed in the isolated local environment).
The measured all-module branch-aware coverage was approximately **77%**, including
unexecuted optional PostgreSQL/Pulsar code and CLI service startup paths. Do not
confuse that with the planned 80% overall / 95% critical-branch release targets.

The tests include all six completion orders, duplicate messages, forged dispatches,
lease expiry and stale owners, broker-send/DB-mark failure, save-before-ACK recovery,
all three deadline classes, explicit sync/async cancellation, early signals,
operator retry, version mismatch, child uniqueness, rollover, delegation and tag
pagination. A subprocess is killed without cleanup after two of three handlers;
a backup is restored and a new process executes only the unfinished handler.
A separate subprocess check varies PYTHONHASHSEED.

Both runnable examples, the CLI help path and local wheel/sdist creation were
executed. The wheel contains Apache-2.0 metadata and the typed marker. Native
services, Ruff and mypy require the configured CI environment and are not marked
locally verified merely because their adapters or commands exist.

## Acceptance coverage against the planning baseline

| Baseline area | Evidence / remaining qualification |
|---|---|
| Durable start and input conflict | Concurrent idempotent starts, wrong-input conflicts, tombstone reuse tests |
| Replay and deterministic ordering | Finished task replay, changed commands, early return, recorded primitives |
| Fan-out/fan-in and duplicates | Three named handlers, six orders, repeated A cannot satisfy B/C, failed-only retry |
| Recovery and atomicity | Memory/SQLite CAS, two coordinators, outbox crash, input ACK crash, restored subprocess |
| Worker fencing | Live-owner exclusion, lease epoch changes, stale results rejected and audited |
| Timers, signals and retries | Separate deadlines, stored backoff, pre-wait signal buffering and ID conflicts |
| Cancellation and controls | Caller timeout distinct from cancel, sync cooperation, async cancellation, audited retry |
| Version and errors | Missing/mismatching workflow blocks; remote error data avoids arbitrary object transport |
| Composition | Child and rollover tests, deterministic race, typed delegated completion including early callback |
| Security and retention | Strict JSON, size/type checks, malformed identity rejection, explicit archive horizon |
| Native adapter behavior | Executable CI integration tests; native production failure matrix still requires qualification |
| Performance/operations | Bounded configuration and diagnostics implemented; workload and 24-hour soak gates remain open |

## Explicit deviations and limits

1. The alpha stores a transactional JSON aggregate per run rather than one table
   per logical object. ADR-001 explains the atomicity benefit and write/polling cost.
2. PostgreSQL is authoritative; completion wake messages are advisory, while the
   coordinator scans committed observations and due work. No separate wake
   subscriber or scheduler service is required for correctness.
3. Durable cleanup across suspended finally/context-manager exits is deliberately
   unsupported. Native asyncio work and arbitrary side effects are task concerns.
4. Bytes-compatible business topics are supported. Schema-managed existing topic
   integration, provider-wide auth policies and DB-free external SDKs need adapters.
5. Signal/command/action quotas are explicit. Old idempotency tombstones are not
   silently deleted. No unlimited-history or unlimited-throughput claim is made.
6. Structured record fields and counters are present; a complete telemetry exporter,
   production alerting package, database rolling upgrades and performance baselines
   are not qualified. No web dashboard or multi-language wire protocol is included.

## Before a production release

Pass CI against the chosen deployed native versions, extend failure injection to
real broker/DB outages and process kills, meet coverage/critical-branch targets,
validate database backup/PITR with broker retention, test rolling application
versions and destructive-control authorization, run representative load/soak
benchmarks, then choose measured resource limits. These are remaining release
gates, not background tasks or promises of later delivery.
