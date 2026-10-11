# Long-running workflows and retention

Protocol 2 supports a logical workflow continuing through successive, bounded
runs. Use `continue_as_new()` before the current run's command/channel limits;
rollover does not automatically rewrite arbitrary Python loops. Keep business
checkpoints in the next run's input and resolve outstanding operations before
continuing. Use a logical handle (`get_handle(..., run_id=None)`) to address the
current run. A handle pinned to an old run does not redirect signals.

## Storage and atomicity

The workflow head contains only the current run. Historical runs and signal
deduplication records live in a separate `records` table, keyed by workflow and
record identity. Transitions point-read the identities they need. Head changes,
auxiliary records, inbox acceptance, and outgoing intent commit together.

Finished current runs retain a small archived head; their full state is read from
the historical record until collection. Start identity remains in the head, so
an old start request cannot create another execution after details are deleted.
The storage bound is per logical workflow at a bounded input rate. Creating new
logical workflow IDs indefinitely still creates new heads.

Default policies:

| Data | Policy |
| --- | --- |
| Message redelivery / supported recovery lookback | 7 days |
| Finished run detail | Eligible 14 days after completion |
| Signal dedup | Through the supplied epoch's retry horizon |
| Task outcomes | Retained while their workflow run is open; at least 14 days after retirement |
| Active runs, outstanding operations | No age-only deletion |
| Start identity | Retained in the logical workflow head |

Larger deployment archive minima are also honored for automatic run/task detail
retention. Changing retention on a running deployment requires a coordinated
policy change; increasing it cannot recover already collected records. All roles
must agree on the retry/recovery contract.

After historical run detail is collected, a query for that old run returns
`NotFound`. A finished current run remains describable as archived, and its
result returns `Archived`. A result follower starting at a collected old run
cannot traverse removed continuation links: reconnect using the logical handle.

## Stable signal epochs

For indefinite operation, supply `signal_epoch` with the signal ID. It is the
integer UTC day in which the logical signal was created. Persist **both** fields
in the sender's outbox and reuse them on every retry, including retries from a
different process or after midnight. Derive it from the event's creation time,
not the retry time:

```python
signal_epoch = int(event_created_at.timestamp() // 86400)
await client.signal(
    workflow_ref,
    "inventory-service",
    inventory_channel,
    event_payload,
    signal_id=event_id,
    signal_epoch=signal_epoch,
)
```

`WorkflowHandle.signal()` and `Client.signal_tagged()` accept the same argument.
The CLI accepts `signal ... --signal-id ID --signal-epoch UTC_DAY`.

An epoch expires at the **end of its UTC day plus seven days** (a 7–8 day window).
Future or expired epochs are rejected. Within a valid epoch, the same ID and
payload is a duplicate; different content is a conflict. A different epoch is a
different identity. Workflow-originated signals receive their epoch when the
engine commits the send command, and relay retries preserve it.

For compatibility, omitting the epoch keeps the original permanent, chain-wide
identity behavior and its **10,000 unique-ID limit**. It never silently picks a
new epoch on a later retry. This bounded compatibility path is not the indefinite
signal API. The same limit applies to epoch-less tag fan-out jobs. Existing
inline signal identities and in-progress tag jobs are moved on access, preserving
their original fingerprint/offset semantics. New epoch-aware workflows do not
copy dedup keys into replay snapshots or successor runs.

Task-tag cancellation requests keep their existing 10,000-request identity quota
per task tag. Their snapshots and closed-task markers are stored separately from
the hot member index; completed snapshots release their target list. Cancellation
fan-out retains the original admission deadline across SDK retries.

## Expired messages and long waits

Engine and SDK publications carry an immutable `_df_expires_at` admission
deadline. Inbox rows with such a deadline can be removed after it passes because
the same envelope will be rejected before state mutation. Old envelopes without
a deadline are kept conservatively; their inbox records are not age-collected.

Delayed messages start their retry window at their scheduled delivery time, not
when the timer was created. A 30-day sleep is therefore still a 30-day sleep.
Already claimed long-running tasks can renew/reclaim their leases; admission
expiration does not cancel an active task. Expired first-time execution commands
fail closed through the protocol-error/DLQ path and require operator diagnosis.

Do not edit an expired message's deadline or epoch and call it a retry. That is a
new authorization to apply an operation and is outside the dedup/recovery
contract. Clocks must use UTC and should not jump backward. PostgreSQL evaluates
admission time after acquiring the workflow/task state row lock.

## Task retirement and recovery

A completed task's outcome is **not** deleted simply because it is old: an open
run can still need it after a long wait or recovery. When a run finishes or
continues, the engine atomically emits `retire_task` for its dispatched attempts.
The task journal records retirement and sets a collection date
covering the retirement time, original execution admission deadline, outstanding
lease/delegation, and retention period. Cached outcomes remain reusable before GC.

Retirement is an idempotent cleanup instruction rather than an expiring business
command. It does not cancel an active task or suppress recovery of an outcome
lost by a task-journal restore; explicit cancellation remains a separate command.
Live leases/delegations extend collection eligibility. Retirement can arrive after
a long worker outage. A very late duplicate can only
recreate an immediately collectible retirement marker; it cannot execute work.
Local claims/heartbeats/finishes rely on their lease/generation fences rather than
creating an inbox row for every heartbeat.

Restore only within the supported seven-day lookback, fence old writers first,
and follow the existing PITR procedure, including explicit retransmission of
known lost signals with their original IDs and epochs. A DB restore does not
rewind Pulsar or external business effects. This release does not promise
arbitrary DB/broker rollback reconciliation. Independent task journals must use
the same recovery/retention contract.

## Garbage collection

Each outbox relay runs bounded GC approximately every ten minutes. A full batch
causes another pass after one second to catch up. Each pass handles at most 1,000
records per table; PostgreSQL locks parent state rows and skips locked rows.
Pending outgoing intent protects its owning state's records. A state row is not
deleted while child inbox/auxiliary records remain. Unknown legacy outbox owners
conservatively postpone collection until those publications drain.

Inspect or execute a batch against **each** workflow, task, and tag journal:

```bash
duraflow --database "$DATABASE_URL" --schema "$MESSAGE_SCHEMA" gc --limit 1000
duraflow --database "$DATABASE_URL" --schema "$MESSAGE_SCHEMA" gc --limit 1000 --apply
```

The default is a dry run. Counts describe currently eligible rows, not all
historical rows. Deleting children can make a state eligible in the same applied
batch or a subsequent pass. Outbox failures, missing retirement instructions,
and legacy records can intentionally retain data; inspect these before assuming
that a smaller TTL will help.

Prometheus exposes `duraflow_gc_deleted_total{role,kind}` and
`duraflow_transport_resources{role,kind}`. Labels never contain workflow IDs,
signal IDs, or reply topic names. Monitor outbox age and database size alongside
these counters; deletion counters alone do not establish a storage plateau.

## Reply resource lifecycle

Producer caches use idle expiry (default 300 seconds, checked during publication)
and LRU eviction under capacity pressure. Native creation/send/close ownership
survives caller cancellation until the actual native operation completes. Busy
producers are not evicted; exhausted busy capacity gives retryable backpressure.
Reply declaration cache eviction only forgets local metadata and never deletes
a broker backlog. Workflow consumer ordering declarations remain pinned.

`Client.close()` prevents new requests; the last outstanding request performs
subscription cleanup. Ensure-only subscriptions can also be unsubscribed, and a
failed cleanup can be retried by calling `close()` again. Reuse clients where
practical, but correctness no longer depends on fewer than 256 lifetime clients.

Broker topic/subscription cleanup is separate from local cache cleanup. Configure
inactive subscription expiration, reply-message TTL, and inactive-topic deletion
for reply routes in Pulsar. Prefer a dedicated reply namespace via
`Topics(..., reply_environment="replies")` on clients, with matching publish and
consume permissions. Keep business/timer namespaces under their own retention
policy; a seven-day blanket broker TTL would break long delayed messages.

## Initialization and qualification

Message schema versions are PostgreSQL **4** and SQLite **3**. Initialize fresh
schemas before starting DB-backed roles. PostgreSQL upgrades are transactional
under the existing migration lock; SQLite upgrades acquire a write lock. Older
binaries must not be run against the new schema. Production data was not present
for this change, so no online rolling-upgrade/backfill guarantee is claimed.

Validation includes auxiliary-record/outbox rollback, 11,000 signals across
rollovers, old-run lookup, epoch expiration after GC, task execution rejection
after journal GC, long timers, and native send cancellation during eviction.
Run `make check`, native integration, and fault qualification on isolated
services before release. Short tests and accelerated clock tests do not establish
a 72-hour RSS bound or a seven-day wall-clock recovery qualification.

The lifecycle soak uses real Pulsar/PostgreSQL, three broadcast subscriptions,
join→publish→rollover, duplicate signals, short-lived clients, bounded GC batches,
and role-loop restarts. It records hot-document bytes, table counts, cache counts,
RSS, and external-effect calls. Role loops share one process; use the existing
message soak/fault suite for independent process and service failures.

```bash
PYTHONPATH=src:. python -m scripts.lifecycle_soak --seconds 259200 --output lifecycle-72h.json
```

Set `DURAFLOW_TEST_POSTGRES` and `DURAFLOW_TEST_PULSAR` to isolated qualification
endpoints. The script owns fresh schemas and removes them on exit. Detailed
records naturally grow during their first 14 days; a 72-hour run does not prove
that all database storage has reached steady state.
