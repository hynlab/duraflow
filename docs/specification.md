# Specification and acceptance baseline

This is a condensed repository edition of the original design plan. The complete
functional requirements, nonfunctional requirements, acceptance-test catalog and
implementation-package gates below are retained from that plan. Detailed current
execution contracts live in [architecture.md](architecture.md), operational
procedures in [operations.md](operations.md), and verified versus unqualified
features in [implementation_status.md](implementation_status.md). A requirement
in this baseline is not a statement that its production acceptance gate passed.

## Mandate

Build an independent Python durable workflow engine under Apache-2.0. Infinitic
is a high-level architectural reference only: do not translate or copy upstream
code, tests, comments, schemas, or fixtures. Public APIs, persistence protocols
and behavioral tests are independently authored. There is no promised JVM
wire/storage compatibility or official affiliation. Application workflows must
not import worker implementations merely to reference their contracts.

The initial infrastructure target is PostgreSQL plus Pulsar, with infrastructure-
free test adapters. Use preserved function decorators, typed references, async
workflow functions, cold operation descriptors and small structural protocols.
Never persist interpreter stacks, closures, arbitrary Python objects or executable
payloads. Workflows contain deterministic orchestration; external effects belong
to tasks. Durable state is authoritative, and message delivery is at least once.

## Design decisions and boundaries

- SDK, replay interpreter, coordinator, runner and infrastructure adapters have
  separate responsibilities, not necessarily separate deployment processes.
- Stable run/command/task identities are distinct from delivery IDs, retry
  attempt numbers, physical consumer processes and execution-owner epochs.
- Record command descriptors, results/errors, waits, policies and pinned build/
  codec/runtime versions. Replay verifies existing commands rather than repeating
  completed business work. Changed code either uses the original pinned version
  or blocks with an explicit mismatch; it never deletes history to proceed.
- Commit accepted state changes, history, inbox deduplication and outbox messages
  atomically. No transaction spans user code or a broker call. After a send-before-
  mark crash, the same event identity may be published again.
- Broadcast freezes the required logical handler set before publishing a single
  business message. Distinct subscriptions are different participants; replicas
  in one subscription are not. Retry only failed handlers, not the broadcast.
- A runner claims a fenced execution lease and persists its validated outcome
  and completion intent before ACK. A broker ACK is not business completion.
- Application retries, transport redeliveries, lease takeovers and deadlines are
  separate mechanisms. Late/superseded observations cannot overwrite accepted
  outcomes. External effects may repeat after an effect-before-record crash;
  expose a stable application idempotency key rather than promise exactly once.
- Timers, pre-wait signals, cancellations and operator interventions are durable.
  Cancel is cooperative; terminate is orchestration termination, not rollback.
  Explicit task compensation is different from runtime coroutine retirement.
- Child creation and rollover are idempotent. A rollover moves pending signals
  and the logical workflow head atomically. Task delegation uses a hashed bearer
  token, expiry and typed result validation with attempt/owner fencing.
- Keep dormant workflows as persisted data, not one live coroutine/thread each.
  Bound payloads, queues, execution concurrency and histories; benchmark before
  advertising throughput or memory figures.

### Recorded implementation deviations

The initial PostgreSQL implementation uses one CAS-protected JSON aggregate per
run, rather than normalizing every history/task/signal/outbox object into a separate
table. The coordinator polls persisted outcomes and deadlines; completion wake
messages are advisory. SQLite was added solely for local development and fresh-
process crash/restore tests. These decisions preserve the intended atomic state
contract but have write-amplification and scheduling-scan costs. ADR explanations
and current unsupported Python constructs are in architecture.md.

## Public API inventory

| Area | Contract |
|---|---|
| Definitions | `@workflow(name, version, build_id)`, `@task(ref)`, `Registry` |
| References | `TaskRef`, `WorkflowRef`, `TopicRef`, `HandlerRef`, `SignalRef` |
| Workflow calls | `ctx.call`, `ctx.gather`, `ctx.broadcast`, `ctx.publish` |
| Durable waits | `ctx.sleep`, `ctx.wait_signal` |
| Recorded primitives | `ctx.now`, `ctx.uuid` |
| Composition | `ctx.child`, `ctx.race`, `ctx.continue_as_new` |
| Client | durable idempotent start, handles, logical heads, list/tag controls |
| Handle | result, describe, history, signal, audited cancel/terminate/retry/resume |
| Runner context | stable idempotency key, heartbeat/progress, cancellation, delegation |
| Errors | conflict, missing run, blocked execution, workflow/task failure, schema/replay errors |

Normal decorated function calls stay local. Remote execution is explicit through
context operations. Types use dataclass-compatible JSON contracts with strict
validation. A coroutine activation is disposable; retirement must not emit durable
cleanup commands. Native asyncio tasks or third-party awaitables are not implicitly
made durable. Input, output and configuration are validated before publication.

## 4. Functional requirements

| ID | Requirement |
|---|---|
| FR-01 | Register versioned workflows and tasks without a framework base class. |
| FR-02 | Start a workflow durably with a caller-provided idempotency key. |
| FR-03 | Invoke tasks by typed references without importing worker implementation code. |
| FR-04 | Run ordinary sequence, branching, bounded iteration, pure helpers, and task-error handling. |
| FR-05 | Execute parallel operations and join their distinct outcomes deterministically. |
| FR-06 | Recover a workflow from persisted history in a fresh interpreter. |
| FR-07 | Fan out one logical business message to a fixed set of named handlers and join results. |
| FR-08 | Report task outcomes automatically through the runner. |
| FR-09 | Handle retries, transport redelivery, leases, and late outcomes as distinct mechanisms. |
| FR-10 | Persist timers, signals, cancellation requests, and operator actions. |
| FR-11 | Pin workflow code/runtime semantics and payload contracts for existing runs. |
| FR-12 | Expose state, history, waiting reasons, attempts, and progress through the SDK and CLI. |
| FR-13 | Support child runs, race, rollover, tags, and delegated completion in staged releases. |
| FR-14 | Run contract-identical semantics against test and production adapters. |


## 14. Operational and nonfunctional requirements

| ID | Required behavior |
|---|---|
| NFR-01 | No accepted logical state change is lost after a successful engine DB commit under the stated durable storage assumptions. |
| NFR-02 | Duplicate/reordered messages cannot advance a logical invocation or join twice. |
| NFR-03 | Pending work is recoverable after engine/runner restarts without retaining live workflow coroutines. |
| NFR-04 | No dedicated thread/process per dormant workflow; memory grows only with bounded active work and caches, not retained suspended stacks. |
| NFR-05 | Bound receive queues, in-flight work, DB pools, replay work per activation, and payload sizes. |
| NFR-06 | Provide reproducible correctness, load, and restart benchmarks before publishing performance claims. |
| NFR-07 | Old supported protocol/history fixtures continue to load and replay after compatible upgrades. |
| NFR-08 | Reject untrusted code loading, malformed payloads, wrong identities, and unsupported schema versions. |
| NFR-09 | Engine and user-task implementations can be tested without Pulsar or PostgreSQL. |
| NFR-10 | Native adapter failure must not block the event loop or make shutdown leak unbounded work. |

Provisional configurable defaults for measurement: worker in-flight limit 32, receive queue 64 per subscription, database pool 5 per process, maximum inline payload 256 KiB. These are starting settings, not demonstrated optimums. Size totals include the number of subscriptions/processes. Validate overflow before publication. Larger payloads require explicit external references or a later artifact-store adapter; do not silently put unbounded blobs in history.

Expose structured correlation logs; run/command/task/attempt IDs; wait reasons; retry counts; last heartbeat; ready-work age; outbox lag; timer lateness; replay duration; stale outcome count; poison-message count; and DB pool saturation. Metrics must not use unbounded run_id labels. Replay-aware logging suppresses duplicate informational logs by default while retaining a debug mode.

Health checks separate process liveness from readiness. Shutdown stops receiving/claiming, drains within a configured limit, persists outcomes when possible, and leaves unfinished leases/messages recoverable. Secrets are never serialized into workflow state by default. Require explicit broker/DB security configuration for nonlocal deployment. No arbitrary internet-exposed task execution endpoint is provided.

A diagnostic CLI supplies run list/describe/history, task attempts, blocked reasons, signal, cancel, terminate, and audited retry/resume. Read-only operations require no dashboard. Destructive commands identify namespace/run and require an explicit operator intent flag. Tests must verify restore procedures, not merely backup creation.

## 15. Acceptance test catalog

These are independently authored behavioral tests. In-memory tests validate semantics; fresh-process durability requires the PostgreSQL integration suite.

| ID | Scenario | Required result |
|---|---|---|
| AT-01 | Repeat start request; then reuse its key with changed input | Same handle first; conflict second |
| AT-02 | Run sequence/branch/loop, then replay from input/history | Identical commands and output |
| AT-03 | Replay a completed task operation | Reuse result; zero new task dispatches |
| AT-04 | Change target/input/order or return early against history | Block with exact mismatch location |
| AT-05 | Durable suspension inside try/except/finally patterns | No cleanup commands from retirement; unsupported constructs rejected/documented |
| AT-06 | Feed a third-party awaitable or asyncio sleep into workflow | Fail safely; do not silently make it durable |
| AT-07 | Concurrent engine replicas propose the same next command | One logical command and one outbox identity |
| AT-08 | Crash before/after DB commit and before/after broker ACK | Recover without lost accepted command |
| AT-09 | Broker ACK succeeded but outbox delivery mark lost | Republish same identity; downstream deduplicates |
| AT-10 | Same task delivered concurrently to two runners | One valid live lease; stale owner fenced |
| AT-11 | Worker finishes external effect then crashes before observation | Possible repeat is documented; stable business key enables app deduplication |
| AT-12 | Result observation committed then worker dies before ACK | No re-execution; completion event recoverable |
| AT-13 | One broadcast, three independent subscriptions | Three distinct logical handler invocations |
| AT-14 | A completes three times; B/C incomplete | Remain at 1/3 |
| AT-15 | A/B complete, engine dies, C completes later | Recover and advance once logically |
| AT-16 | Unexpected fourth handler or process replacement | Reject unexpected handler; replacement retains same logical identity |
| AT-17 | B fails after A/C succeed | Retry B only, without a new broadcast |
| AT-18 | Required subscription missing before publication | Explicit preflight/configuration failure |
| AT-19 | Handler is offline after proper provisioning | Wait until recovery or documented deadline; never reduce expected set |
| AT-20 | Broker redelivery during application retry | No unintended extra application attempt |
| AT-21 | Old attempt/lease result arrives late | Audit/ignore; cannot overwrite newer decision |
| AT-22 | Completion races timeout/cancel on multiple replicas | One accepted terminal transition; identical replay |
| AT-23 | Engine restarts with overdue timers | Due work fires logically once, never early |
| AT-24 | Signal arrives before wait; duplicate signal; signal timeout race | Buffer, deduplicate, and replay accepted order |
| AT-25 | Cooperative cancel of async task and noncooperative sync task | Correct distinct cancellation/termination outcomes |
| AT-26 | Old workflow remains active during new-version deployment | Continue with pinned implementation or block safely |
| AT-27 | Retry exhaustion raise versus block; operator retry | Correct policy; audit; no history deletion |
| AT-28 | Child-start duplicate and parent-close race | One child per command; documented close behavior |
| AT-29 | Rollover races signals and repeated start requests | No lost signal or duplicated logical start |
| AT-30 | Invalid schema, huge payload, malicious type name, credential leakage | Reject/quarantine; no code execution or secret logging |
| AT-31 | DB/broker disconnect, restart, and graceful shutdown | Recovery without unbounded queues or leaked workers |
| AT-32 | Tombstone/retention expiry, archive lookup, backup restore | Explicit guarantee boundaries and validated recovery |
| AT-33 | Different PYTHONHASHSEED and supported Python versions | Compatible deterministic command fingerprints |
| AT-34 | Conflicting payload for an already-seen event ID | Integrity error; not silently treated as harmless duplicate |
| AT-35 | Kill all application processes while two of three handlers completed | Fresh processes recover solely from persisted state/transport |

Property-based tests generate duplicate, delayed, reordered, stale, and concurrent events. Invariants include monotonic accepted history, immutable terminal state, unique command identity, no double signal consumption, and no join before the expected success set is complete.

## 16. Implementation work packages

Each work package is a mergeable unit with requirements, tests, and documentation. Do not scaffold every module first and postpone integration until the end.

| Package | Work | Dependencies | Exit gate |
|---|---|---|---|
| P0 | Requirements, API examples, reference/provenance policy, support matrix, identity/state ADRs | None | All initial semantics and non-goals written; API sketches type-designed |
| P1 | IDs, events, command descriptors, codecs, public errors, reducer skeleton | P0 | Unit/property tests for identities and transition invariants |
| P2 | Coroutine driver, deterministic grouping, replay, error injection, retirement | P1 | AT-02–06, AT-33 pass using fresh interpreter instances |
| P3 | In-memory engine/runner/client vertical slice | P2 | Start -> task -> result -> second task -> completion; manual event duplication tested |
| P4 | PostgreSQL schema/migrations, revision commits, inbox/outbox, claims, signals/timers storage groundwork | P3 | Fresh-process restart and concurrent commit tests; no broker needed for core proof |
| P5 | Pulsar adapter, bounded bridges, runner outbox/ACK, broadcast and targeted retry transport | P4 | AT-08–19 and AT-35 pass against real broker/database |
| P6 | Persisted policies, due scheduler, retry/lease recovery, signal wait, cancellation, blocked recovery, version deployment | P5 | Lifecycle and race tests AT-20–27, AT-31 pass |
| P7 | Child runs, race, rollover, delegated completion, tag controls, compensation examples | P6 | Composition tests and independent histories pass |
| P8 | Security limits, retention/archive, diagnostics/metrics, recovery runbooks, compatibility fixtures | P6/P7 | AT-30–34, migration/restore/replay suite and soak gates pass |
| P9 | Distribution, clean installs, reproducible examples, support policy, alpha-to-1.0 release review | P8 | Wheel/sdist/license metadata verified; published support matrix matches tested environments |

P4 establishes data structures for timers/signals before the full P6 public lifecycle. P5 may release a clearly labeled distributed alpha only; version 1.0 requires every applicable later gate.

### Per-package execution discipline

Specify the behavior and acceptance test before coding. Implement a narrow vertical slice, run the targeted tests, then the full quality gate. Update the implementation-status matrix with evidence rather than aspirational checkmarks. Extract extra abstractions only when a concrete caller needs them. Make one change category per reviewable unit.

No implementation PR may replace a mandatory crash/race test with a coverage number or mock-only success. Freeze public/wire contracts before parallelizing implementation across adapters. A solo workflow should prioritize dependency order over simultaneous branches that constantly change shared interfaces.

## Release discipline

Implement vertical slices in dependency order; each includes its acceptance tests
and documentation. Keep code, specifications and agent instructions in English.
The initial working distributed build may be labeled alpha, not automatically
version 1.0. Complete functional code and passing a native happy-path test do not
replace native crash, restore, soak and performance qualification.

The planned release gate includes formatting, lint, static typing, unit/property
checks, real-infrastructure tests, compatible-history fixtures, secret/dependency
audit and explicit provenance review. Coverage targets are at least 80% overall
and 95% branches for critical transition/replay logic, but a percentage never
substitutes for a mandatory crash/race test. Record actual measured coverage.

Build and inspect wheel and source distributions, the typed marker, applicable
license files and required runtime migration assets. Optional PostgreSQL and
Pulsar imports must not be required for infrastructure-free use. Clean installation
and examples must work on the stated Python/platform matrix. PyPI publication,
package-name reservation and release tags require explicit authorization and
credentials; they are not implicit in this implementation task.

Representative qualification workloads include sequential tasks; 3/10/100-way
fan-out; 1,000/10,000 dormant workflows; repeated redelivery/failure; rolling
restarts; maximum allowed payloads; and a 24-hour soak. Record hardware, topology,
versions, latency percentiles, engine overhead, throughput, memory, queue depth,
DB I/O and recovery time. No unmeasured performance claims or automatic cleanup
of live histories are permitted. Keep all remaining gates visible in the status
document, without marking them completed merely because the corresponding API
or test file exists.
