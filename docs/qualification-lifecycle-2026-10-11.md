# Lifecycle hardening verification — 2026-10-11

Verified the working-tree lifecycle changes based on `3c6eb4d`. This is local
verification of the changes, not a published release or a claim of multi-day
production qualification.

## Executed checks

| Check | Result |
| --- | --- |
| `make check` | Format, lint, mypy and 347 non-integration tests passed |
| `make native-check` | 407 tests passed, no failures/skips; includes the unit tests |
| Codec matrix | Frozen fixtures passed under Pydantic 2.13.4 and 2.13.5 |
| `make native-fault-check` | 16 destructive fault tests passed, no failures/skips |
| `make production-gate` | Passed, including the new record/signal modules |
| `make build` | sdist and wheel built successfully |
| `git diff --check` | Passed |

The 407 general/native tests plus 16 fault tests cover **423 distinct tests**.
Native checks used local PostgreSQL and Pulsar qualification services. Fault
checks verified container ownership and loopback ports before interrupting the
isolated `duraflow-qualification-release-local` project. They included physical
PITR with restored/current task journals, explicit signal replay, process loss,
service kill/pause, and stale-owner fencing. SQLite `ResourceWarning` diagnostics
were emitted by the qualification suite; tests still completed successfully.

JUnit/coverage outputs are `test-results.xml`, `native-results.xml`,
`fault-results.xml`, `coverage.xml`, and `coverage.json` (local generated artifacts).

## New regression evidence

- 300 distinct reply routes through a cache limited to three entries.
- Native Pulsar reply churn and ensure-only subscription deletion.
- Producer pinning while a cancelled native send is still executing.
- Client close during reply provisioning, with eventual cleanup.
- Auxiliary record/state/outbox rollback on outgoing identity conflicts across
  memory, SQLite, and PostgreSQL.
- 11,000 signals across 11 rollovers, bounded current-run document size, old-run
  lookup, cross-run deduplication, and rejection after epoch expiration/GC.
- Legacy signal fingerprint migration and in-progress tag-page preservation.
- PostgreSQL admission time sampled after the parent row lock.
- GC progress past an unrelated key protected by pending outbox work.
- Old task execution rejection after retirement and journal GC.
- Retirement arriving after the ordinary message retry horizon.
- Retirement from an earlier DB timeline not suppressing recovery of a lost task
  outcome; live heartbeats protecting retirement eligibility.
- Late heartbeat not recreating a collected journal parent.
- Task-tag retries retaining their logical identity and original cancellation
  admission deadline.
- A 30-day timer surviving accelerated-clock GC.

## Real-service lifecycle soak

Executed `scripts.lifecycle_soak` for a requested **180 seconds** (184.522 seconds
including startup/finish). The scenario uses three broadcast subscriptions,
join→next publication→rollover, duplicate signal requests, a fresh client per
generation, GC passes, and settled-work role-loop replacement.

| Measurement | Result |
| --- | --- |
| Completed continuation generations | 49 |
| Accepted external effects | 147; one call per effect key |
| Role-loop restarts | 4 |
| Maximum sampled hot head document | 3,405 bytes |
| Producer cache | Reached and stayed within the configured 32-entry limit |
| Subscription declaration cache | Reached and stayed within the configured 32-entry limit |
| Final outboxes | Drained |

The soak's roles share a process. Independent process/service failure coverage
comes from the separate native/fault suite. The script stores only bounded
telemetry samples and uses a disk-backed verification ledger.

## Remaining operational qualification

A 72-hour soak, actual seven-day recovery-window exercise, post-retention database
steady-state measurement, and multi-host broker/quorum/replica promotion exercises
were **not** run in this session. The short soak does not establish an RSS upper
bound, maximum throughput, or steady-state database size. Historical detail is
expected to grow during its initial retention interval.

See [the lifecycle runbook](long-running-workflows.md) for the signal-epoch
contract, GC operations, schema versions, and longer soak invocation.
