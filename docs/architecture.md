# Architecture and execution contract

> Historical protocol-1 execution contract. The current message-driven runtime
> is documented in [the numbered architecture guide](../guide/8_architecture.md).
> Existing protocol-1 histories retain the rules below during cutover.

## Public interfaces

`@workflow(name, version, build_id)` marks an async `(context, input)` function;
`@task(ref)` marks a sync or async `(input)` or `(context, input)` function.
Both preserve ordinary function invocation. TaskRef/WorkflowRef declarations
contain a stable name/version and input/output contracts. Use a separate small
contracts module for independent application worker repositories.

WorkflowContext operations are cold, single-use descriptors: call, gather,
broadcast, publish, sleep, wait_signal, now, uuid, child, race, continue_as_new.
Groups accept engine descriptors, not arbitrary coroutines. Nested groups and
rollover inside groups are rejected; use child workflows. Gather and broadcast
wait for all members and preserve declaration order. A failure becomes TaskFailure
or GROUP_FAILURE with serializable causes. No arbitrary exception objects cross
the wire.

A broadcast returns BroadcastResult, indexed by the declared HandlerRef. Race
returns RaceResult(index, value), or raises its recorded winning failure. Losing
tasks/timers continue unless explicitly cancelled; child close behavior is
separate. Results are selected by the coordinator's **committed acceptance
sequence**, not worker timestamps. Terminal workflow results never change when a
loser finishes later.

Client offers durable start, get_handle/current/list, buffered signal delivery,
external completion and paginated tag controls. Handles provide describe,
result/history, signal, audited cancel/terminate/resume/retry, and explicit
archive. `result(timeout=...)` limits only the caller's wait. Logical workflow IDs
and idempotent request IDs cannot be reused for a different execution. An
intentional rerun needs a new identity. `continue_as_new` alone changes the current
run behind a logical workflow ID atomically.

## Replay

A fresh coroutine is reconstructed from a fresh copy of input on every activation.
It yields an Operation token at a durable boundary. The replay interpreter
compares command kind, target/version, canonical payload/options and group
membership against committed commands. It feeds recorded results/errors back,
suspends on pending commands, or proposes a new command without I/O. Returning or
raising before an existing history suffix blocks the execution.

Only input, manifest, command descriptors, outcomes, waits and transitions are
stored. No coroutine, frame, closure, open connection or pickle is persisted.
Retirement closes a disposable coroutine in a guarded context; durable finalizer
work is rejected, even if its immediate exception was swallowed. Pure finalizers
may execute repeatedly. Workflow code is trusted and must obey these restrictions;
there is no security sandbox or arbitrary Python loop preemption.

## IDs and routing

Run IDs identify one replay history. Commands use deterministic ordinals and
member positions. Task IDs are UUID5-derived from the run and operation occurrence.
Task attempts number application retries; lease epochs number physical owners of
one attempt. Redelivery does not spend the application retry budget. A subject or
business correlation alone is never a task identity.

Internal routes use an injective length-prefixed namespace plus task contract.
Business-topic subscriptions use an injective namespace plus logical handler.
Topics are reused, never allocated per execution. Broadcast recipients are frozen
before publication and do not change when consumer processes connect/disconnect.
A required subscription is provisioned before the outbox publication is committed.
The temporary provisioning consumer is closed immediately; only a Worker retains
consumers, avoiding idle engine prefetch stealing work.

Internal metadata is versioned JSON in the reserved `duraflow` Pulsar property.
The original declared business JSON payload is preserved. The initial adapter
supports bytes-compatible topics only; existing schema-managed topics require an
explicit compatible codec/schema adapter. Unknown protocols, malformed JSON,
conflicting IDs and undeclared recipients are quarantined, not executed.

## ADR-001: transactional run aggregates

The alpha uses an independent `duraflow` PostgreSQL schema with runs, heads,
requests and schema_version tables. History, task attempts, inbox, outbox, timers,
signals and controls are logically separated inside each run's JSON aggregate.
This intentionally replaces the original plan's normalized table layout with one
short compare-and-swap transaction per run. It preserves atomicity while reducing
cross-table coordination code. A revision conflict discards a proposed activation
and reloads; it never executes a business function under a database lock.

Trade-offs: JSON write amplification, per-run contention and bounded page scans
rather than indexed per-task scheduling. The same aggregate contract is exercised
against MemoryStore and file-backed SQLiteStore. SQLite is a development and crash
test adapter, not claimed to provide PostgreSQL's operational characteristics.
Future normalization needs an explicit schema migration and old-state fixtures.
The v1 schema bootstrap refuses unknown versions; it does not silently migrate.

## ADR-002: PostgreSQL source of truth and outbox

Trusted SDK/engine/runner roles use narrow shared-store methods; task functions
receive no engine connection. Engine commits must not share an arbitrary caller's
business ORM transaction. An accepted start includes a durable wake-up outbox
entry. DB scans reconcile unfinished state, so wake-up delivery is advisory and
not necessary for liveness. The alpha does not run a separate event-consumer
coordinator; it polls persisted observations and due work.

The runner claims a fenced lease, executes outside transactions, validates output,
and commits observation plus completion wake before ACK. The coordinator alone
accepts a logical result. Dispatches must match a committed outbox record exactly.
Inbox keys are scoped by logical recipient; three subscriptions must not deduplicate
one another's work.

Outbox publications are claimed by lease, sent outside a transaction, then marked
delivered after broker confirmation. A crash in between republishes with the same
event identity. DB transactions do not also commit Pulsar. A committed runner
observation is reused on input redelivery. Stale observations cannot overwrite a
new owner/result; bounded fingerprints and audit entries retain diagnostic evidence.

Reconciliation re-dispatches only unfinished invocations using their stable task
identity. Failed broadcast handlers retry on their direct task route; the original
broadcast is not repeated for application retries. Lost broker state can likewise
be repaired for tasks from durable invocations. Ordinary publication-only business
messages need broker retention/deduplication appropriate to the application.

## Deadlines, retries, controls

Default application retry count is one. RetryPolicy provides bounded exponential
backoff, error-code selection and raise/block exhaustion. Schedule timeout starts
when a particular attempt becomes eligible; attempt timeout starts on its first
claim; overall timeout starts with the logical invocation. Lease renewal never
extends an attempt's fixed deadline. An overall deadline is never reset by retry.

The coordinator applies cancellation/deadline rules before an observation in the
same activation. A result observed by a runner but not yet accepted can lose to a
deadline. The first successfully committed terminal transition wins. A signal
already buffered in an activation is consumed before a simultaneously due signal
timeout. These are explicit acceptance rules, not claims about physical arrival
ordering across machines.

BLOCKED is not business failure. Missing implementations, mismatched replay,
unknown codecs and retry block policy require audited operator intervention.
Ordinary resume cannot bypass a blocked task; retry_blocked_task preserves past
attempts and appends a new one. Multiple blocked members must each be addressed.

Cancel requests cooperation and stops new scheduling; a sync task may remain
CANCELLING until it exits or a deadline resolves it. Terminate ends orchestration
without proving physical effects stopped. Neither automatically compensates
external actions. Use explicit idempotent compensation tasks for business failure.

## Signals, children, rollover, delegation

Typed signals are buffered before wait registration, deduplicated by signal ID,
and pinned to one schema per channel. Consumed signal-to-command mappings replay
unchanged. Quotas bound mailbox/key growth. Rollover atomically finishes the old
run, creates a bounded-history successor, moves unconsumed signals, retains signal
IDs and updates the logical head. Handles refer to one run unless follow_continued
is requested. signal_workflow resolves/follows the logical head across races.

A child command is committed before idempotently creating its deterministic child
run. Parent waits survive replay and duplicate creation attempts. Parent close
requests child cancellation unless abandon_on_parent_close was set explicitly.
Children retain independent histories and may themselves roll over. Do not roll
over a parent while any of its operations remain unresolved.

An async task may call `await context.defer(timeout=...)`, distribute the resulting
opaque bearer token to its trusted external executor, and return that Deferred
marker. Only the token hash and expiry are stored. `Client.complete_external`
requires the matching TaskRef, strict output validation, attempt/epoch checks,
and idempotent content. Completion may arrive before the task returns. Tokens
are secrets and must not be put in logs. External completion does not bypass the
shared-store trust model.
