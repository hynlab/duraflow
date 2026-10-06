# Phase 4 — connection policy, controls and retention

D01: Production settings require PostgreSQL sslmode=verify-full with an explicit
readable CA and a hostname, and authenticated pulsar+ssl with an explicit CA.
The official Pulsar adapter disables insecure certificates and validates hostname.
DATABASE_URL and PULSAR_TOKEN accept exactly one direct or _FILE source. File
reads are bounded, regular-file-only and reject symlinks, world access, writable
by others and unexpected owners; provision container secrets with mode 0400/0640.
Mounted Kubernetes symlink projections require a reviewed materialization step;
this implementation deliberately does not silently follow arbitrary symlinks.
CLI errors never print raw connection or driver exception text. Configure broker
server-side authorization and private network access independently.

D02: --actor is audit context, NOT authenticated identity. PostgreSQL checks
current_user and actual membership of the configured operator role for controls,
archive and DLQ replay, and records that principal. The SQL deployment template
separates NOLOGIN reader, runtime writer, operator and migration groups. Actual
login accounts and secrets must be provisioned by the operator, not committed.
Readers cannot write even if they bypass the SDK. Runtime writers remain trusted:
they hold SQL UPDATE and can deliberately bypass Python checks. This is NOT
hostile-worker or multi-tenant isolation. Parent cleanup uses an explicit private
runtime path rather than trusting an actor string or granting every worker the
operator role. The schema owner/migration account is not a runtime credential.

D03: Production archive requests must satisfy deployment retention and redelivery
horizon floors as well as the existing terminal/no-pending-work/no-unsent-message
checks. Full payloads and errors are removed only after safe archival; request,
workflow-head, signal-key and action tombstones remain. No automatic destructive
GC, live-history truncation or tombstone expiry is introduced. Backups, broker
retention, external idempotency and audit-data privacy need matching policies.

Tests exercise fail-closed settings, unsafe secret sources, explicit TLS flags,
principal spoofing rejection, retained tombstones and native PostgreSQL reader
and operator roles. These checks do not certify the user's actual certificate
chain, broker ACL configuration or backup/PITR deployment.
