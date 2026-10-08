# Local distributed qualification

The target is the protocol-2 message loop through the actual consumers and journals.
Legacy tests remain required regressions; their evidence does not substitute for
protocol-2 recovery. This catalog describes executable scenarios, not exhaustive
proof over every possible distributed execution.

## Scenario catalog

| ID | Suite | Scenarios and success conditions |
| --- | --- | --- |
| J01 | `test_message_store_contract.py` | Memory/SQLite/PostgreSQL concurrent duplicate inputs apply once; distinct inputs all apply; conflicting input identities preserve state. |
| J02 | Same | Same-batch/existing-row outbox collisions roll back; identical content preserves its lease; integer/float/boolean wire differences are conflicts. |
| J03 | Same | Competing relays claim disjoint pages; wrong owners cannot release/delete messages; literal-prefix pagination and SQLite page-quota exhaustion preserve atomicity. |
| J04 | `test_native_message_migration.py` | PostgreSQL schema 1→2 migration preserves rows, inbox identities and leases; interrupted upgrades roll back and retry; unknown versions fail closed. Numeric exponent/negative-zero round trips use the shared store contract. |
| D01 | `test_message_delivery_faults.py` | Failure before send, after send, or before delivered-mark preserves stable publication identity and one accepted state change. |
| D02 | Same | Failed ACK causes harmless replay; failed DLQ publication retains the original delivery; healthy work subsequently proceeds. |
| D03 | Same | Invalid JSON/envelopes, wrong versions/types, exact 16-MiB boundary, Unicode, early timers, and expired relay ownership. |
| D04 | `test_pulsar_provision_recovery.py` | Native subscription completion after timeout/cancellation closes abandoned provisioning/receiving consumers instead of stranding prefetched messages. |
| W01 | `test_message_worker_edges.py` | Malformed task contracts cannot execute; duplicates during a live lease do not execute twice; cancellation, heartbeat loss, and stale generations cannot commit success. |
| E01 | `test_message_engine_edges.py` | Cross-namespace/topic injection is rejected; an invalid replay decision rolls back even earlier valid task intents; recorded time/UUID and cross-workflow signals survive replay. |
| E02 | `test_message_replay_contract.py` | Unsupported codec/protocol/build, shortened/changed history, exceptions before committed history, and operation budgets reject divergent replay. |
| L01 | `test_message_lifecycle_edges.py` | Schedule/overall/attempt deadlines fence late results; retry exhaustion retains effect keys; parent controls cancel children; compensation, waiter cancellation, namespace isolation, and invalid delegation. |
| T01 | `test_message_tag_recovery.py` | Replacement after page commit resumes cancellation fan-out; duplicate/old controls cannot reschedule pages; closed task IDs do not reappear. |
| M01 | `test_message_stateful.py` | Independent reference model generates deliveries, conflicting identities, rollback, time advances, claims, release, and stale completion on Memory/SQLite. Hypothesis saves/shrinks failures. |
| N01 | `test_native_message_resilience.py` | Two state engines/replay workers with 1/2/4 task workers; concurrent duplicate starts/signals, graceful role restarts, completion/termination races, and process loss after an effect. |
| N02 | Same and `test_message_process_recovery.py` | Fresh-process SIGKILL before/after state commit, after replay/result publication, before/after external effect, after observation/before ACK, and both engines stopped while a signal is pending. |
| N03 | `test_native_message_network.py` | Only the engine's real PostgreSQL TCP connection is cut/reset and later delayed. Signal stays pending through the partition and completes after reconnect, with/without engine replacement. |
| F01 | `test_message_service_faults.py` | PostgreSQL/Pulsar kill and pause, combined with worker replacement, preserve the committed effect and final result. |
| F02 | Same | Stop old task process without killing it, expire lease, unload its isolated Pulsar topic, acquire a newer generation, resume old process and observe its finish being fenced. |
| R01 | `test_message_pitr.py` | Physical base backup + archived WAL restores a waiting workflow; post-target SQL marker is absent; explicitly replaying its acknowledged signal preserves external effects with old/new task journals. |
| S01 | `scripts/message_soak.py` | 30–60 minute real-service workload, deterministic rotating SIGTERM/SIGKILL, exact completed results/effects, final outbox drainage, latency/RSS observations. |
| H01 | `test_pytest_guard.py` | Abrupt pytest exit cannot strand a registered, stopped replay subprocess; released resources are not touched. |

Existing suites additionally cover broadcasts, child rollover, filters, public CLI,
configuration/secret precedence, TLS policy, readonly database roles, codec fixtures,
history retention, package installation, and process supervision.

## Reproduce

From a source checkout with Python 3.12+ and Docker:

```bash
python -m pip install -e '.[dev]'
export COMPOSE_PROJECT_NAME=duraflow-qualification-dev
export DURAFLOW_POSTGRES_PORT=54332
export DURAFLOW_PULSAR_PORT=56650
export DURAFLOW_PULSAR_ADMIN_PORT=58080
export DURAFLOW_TEST_POSTGRES='postgresql+psycopg://duraflow:development-only@localhost:54332/duraflow'
export DURAFLOW_TEST_PULSAR='pulsar://localhost:56650'
export DURAFLOW_ALLOW_DESTRUCTIVE_TESTS=isolated-compose
docker compose up -d --wait --wait-timeout 240
make extended-check
docker compose down -v
```

Use `PYTHON=/path/to/python` to choose the interpreter. Repeat the non-integration
suite on Python 3.13. Run destructive tests and soak **sequentially** against the same
project: interrupting the soak's infrastructure changes the workload being measured.
For an individual native case, use `PYTHONPATH=src:. python -m scripts.pytest_guard
tests/<file>.py -v`. Include `DURAFLOW_REQUIRE_NATIVE=1` for a full required suite;
its guard rejects skips and requires at least two integration cases.

The coverage gate retains the legacy critical-branch 95% floor and global 80%
branch-aware floor, and requires at least 80% branch-aware coverage independently
for each of ten protocol-2 core modules. Coverage is not a substitute for fault tests.

## Evidence boundaries

- A task invocation can repeat after effect-before-result failure. The independent
  SQLite effect ledger enforces the application's stable idempotency key; Duraflow
  does not promise exactly-once effects on arbitrary external systems.
- Protocol-2 PITR here uses **explicit replay of a known lost signal**. Generic
  automatic reconciliation after arbitrary journal/broker rollback is not qualified.
- Native tests use standalone Pulsar and a single PostgreSQL primary on one Docker
  host. Multi-broker/BookKeeper quorum failures, PostgreSQL replica promotion, and
  actual multi-host power/network failures require a separate HA test deployment.
- The TCP proxy qualifies an engine-to-PostgreSQL partition. It does not claim to
  proxy Pulsar's advertised broker lookup or to emulate packet-level UDP loss.
- Soak percentiles describe batches of four workflows on shared local resources.
  They are observations, not capacity/SLO guarantees. Direct role RSS excludes
  replay children; FD/thread/connection leak thresholds and memory/CPU exhaustion
  qualification remain separate work. SQLite storage exhaustion uses a real page quota.
- No test result is inferred from a skip or a historical artifact. JUnit records
  individual outcomes; retain report files and the tested commit together.
