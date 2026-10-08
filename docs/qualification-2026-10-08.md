# Local qualification — 2026-10-08

## Scope and environment

- Work began from `975a941` on a dedicated `test` branch.
- Suite collection grew from **244 to 392 cases** (322 non-integration, 70 integration;
  16 integration cases require the isolated destructive-service opt-in).
- macOS/arm64, Docker Linux/arm64 with 8 CPUs and approximately 8 GiB assigned memory.
- PostgreSQL 16 and Pulsar 4.0.3 in the separate `duraflow-qualification-test` project.
- Successful interpreter matrix: Python **3.12.15** and **3.13.16**; both use SQLite 3.53.1.
- See [the scenario catalog](qualification.md) for assertions and evidence boundaries.

## Executed checks

| Check | Observed result |
| --- | --- |
| Formatter, Ruff, mypy | Passed; 40 source modules checked by mypy |
| Python 3.12 required non-fault suite + coverage | 377 passed; included one redundant state-machine class alias subsequently removed |
| Python 3.13 final required non-fault suite | **376 passed, no skips** |
| Full destructive qualification, Python 3.12 | **16 passed, no skips** |
| Final protocol-2 PITR/service-fault rerun after subscription/cleanup fixes | **7 passed, no skips** |
| Frozen codec fixtures | Passed with Pydantic 2.13.4 and 2.13.5 |
| Expanded production coverage gate | Passed; **85.80% overall branch-aware coverage** |
| Core message PostgreSQL journal | **100% branch-aware coverage** |
| Wheel + sdist build, strict Twine metadata check | Passed |
| Fresh base-wheel installation, dependency consistency, isolated imports and CLI help | Passed |
| Isolated installed-wheel message workflow | Result **22** |
| Documented Linux Compose demo, then engine restart and repeat | Result **22**, then **22** |
| Independent code review | Required findings addressed and reviewed; no unresolved Required findings |

The non-fault suite includes the unit suite. Counts in different rows overlap and
must not be added to claim a larger set of distinct tests. The final 392-case
collection removes the duplicate state-machine alias without removing a scenario.

## Sustained distributed runs

Two 30-minute workload runs were performed during this qualification session, each
with two state engines, two replay workers, two task workers, and both tag roles.
The workload uses batches of four workflows with a signal between two external effects.

| Observation | First run | Later run |
| --- | ---: | ---: |
| Requested workload duration | 1,800 s | 1,800 s |
| Total duration including setup/cleanup | 1,810.253 s | 1,813.389 s |
| Completed workflows | 1,468 | **1,656** |
| Accepted external effects | 2,936 | **3,312** |
| Later-run role SIGTERM/SIGKILL events | — | **83** |
| Batch latency p50 | 4.434 s | **4.054 s** |
| Batch latency p95 | 8.318 s | **6.733 s** |
| Batch latency p99 | 12.221 s | **8.622 s** |
| Final outbox drained | Yes | Yes |

Both runs preserved exact expected results and accepted-effect counts. These are
session workload observations on shared local hardware, not throughput guarantees
or a claim that every role was saturated. `task-tags` was an idle restart participant
in this workload; separate tests exercise task-tag cancellation and paginated recovery.
The final source's cancellation/subscription boundary changes were additionally
verified by the required native suites and focused fault rerun.

Local artifacts: `native-results.xml`, `fault-results.xml`, `coverage.json`,
`coverage.xml`, `soak-results.json`, `qualification-artifacts/native-313.xml`, and
`qualification-artifacts/protocol2-fault-final.xml`. Later-run process logs are in
`qualification-artifacts/soak-q984j2xt/`. These generated files are intentionally
ignored by Git; this report records the results alongside the source changes.

## Defects and test-environment failures uncovered

- Fixed outbox collision atomicity and inconsistent duplicate handling across stores.
- Preserved numeric wire representations using PostgreSQL message schema 2 (`JSON`),
  with transactional schema-1 migration and rollback/retry tests.
- Fixed loss of redelivery when DLQ publication fails.
- Rejected malformed service contracts as protocol errors instead of repeated `KeyError` failures.
- Closed late native consumers after provisioning/receive subscription timeout or cancellation.
- Corrected recovery fixtures to wait for actual broker reconnection and to clean
  owned process groups, restored containers, and primary-side WAL/base backups.
- An older host **Homebrew Python 3.13.3 / SQLite 3.51.0** combination intermittently
  stalled inside concurrent `sqlite3.connect()`/`close()` and did not qualify.
  The maintained Python 3.13.16 / SQLite 3.53.1 environment passed the full non-fault suite.
- A newly created environment initially contained an incomplete cached `certifi`
  installation. Reinstallation without the cache restored `certifi.where()`; the
  subsequent full Python 3.13 native run passed. No application workaround was added.

## Operational notes

Initialize every PostgreSQL message-journal schema before restarting updated roles;
see [schema-2 upgrade instructions](../guide/6_operations.md#postgresql-message-journal-schema-2).
PITR qualification covers explicit replay of a known acknowledged signal with a
restored or current task journal. It does not establish generic automatic rollback
reconciliation. Multi-host/HA quorum and replica-promotion qualification remain
outside this single-host standalone-service matrix.
