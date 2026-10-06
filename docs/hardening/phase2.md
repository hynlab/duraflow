# Phase 2 — storage and scheduling

B01: explicit transactional PostgreSQL schema migration 1 -> 2. Existing JSON
run documents are preserved; next_due/status/implementation projections and a
partial due-work index are added. Unknown ledgers and downgrades fail closed.
The CLI init/migrate operation is separate from runtime startup. Stop old runtime
writers before migration: this is a bounded maintenance upgrade, NOT an online
mixed-alpha/new-runtime migration. Multiple workflow builds on the new runtime
are independent of this database runtime-version restriction.

B02: internal state mutations use SELECT FOR UPDATE and sample PostgreSQL
clock_timestamp AFTER acquiring the row lock. A context-local trusted clock
makes claim, heartbeat, observation and operator changes use that same clock.
No user callback, broker call or replay runs inside these transactions. Workflow
advancement remains optimistic CAS, with database time sampled for deadline
checks. Request creation and rollover creation are stamped by PostgreSQL.

B03: runtime polling selects due run IDs using an indexed projection and exact
manifest capability filter. Manual SDK list/scan remains separate. Finished
status alone never suppresses pending outbox, cancellation or child-close work.
Signal/task observations update the projection atomically. Quiet completed runs
and long sleeps are excluded; a conservative child recheck remains bounded at
one second. Missing child records remain reconciliation obligations because a
concurrent child start might still commit.

B04: PostgreSQL pool/lock/statement/operation bounds; native Pulsar calls run in
a bounded eight-thread pool. Cancelling a caller does not release a native-call
slot until that call actually exits. Producer sends have a native five-second
timeout. Outbox retries persist 1..60 second backoff and sanitized error class.
The next send retains its stable event ID. Engine batches process at most four
runs concurrently, with per-run 30-second bounds. There is no infinite immediate
retry loop and one failed route cannot permanently monopolize other runs.

These changes do not establish PITR, broker disaster recovery or production
capacity. Those remain separate native-failure and load acceptance tasks.
