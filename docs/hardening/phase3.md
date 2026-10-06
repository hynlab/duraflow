# Phase 3 — operational tooling

C01: RuntimeSettings validates finite limits, timeout ordering, pool sizes and
lease margins. DURAFLOW_* environment variables and selected CLI flags configure
the runtime; --production rejects SQLite fallback. URL fields are hidden in repr.
Connection-security requirements are the next phase, not implied by this flag yet.

C02: service supervision stops admission before draining, keeps in-flight work
within the grace budget and bounds cleanup. The dedicated CLI process exits 75
when a task/native call cannot drain; library supervision raises ShutdownTimeout
and never kills its host process. Arbitrary Python threads cannot be killed safely.
A CPU-bound async task that blocks the entire event loop still requires the
external container/service supervisor's hard stop deadline. No rollback of an
external side effect is claimed. Task result-before-ACK ordering is unchanged.

C03: optional bounded read-only /live, /ready and /metrics probes. Readiness checks
schema, broker lookup and local registered implementations, expires when stale,
and becomes false before drain. Runtime admission pauses when dependencies fail.
Probe requests never consume business messages. Bind probes to a private network.

C04: explicit event names and allowlisted scalar correlation fields only. No
payloads, arbitrary exception strings, traceback locals, URLs or tokens are emitted
by the operational formatter. Applications remain responsible for their own logs.

C05: bounded role/status metric labels, runtime counters, due-deadline lag and a
clearly named 100-active-run sampled outbox-age gauge. This sample is not a global
proof that no older outbox exists; use per-run diagnostics when investigating.
Prometheus alert rules are provided for readiness, errors, blocked runs and delay.

C06: dead-letter envelopes retain bounded raw bytes, original metadata, source
route and checksums. Oversized payloads are hash-only/truncated and are NOT
replayable. dlq-peek uses a separate inspection subscription and does not ACK or
advance the operational queue. Payload display requires an explicit flag.
dlq-replay requires an explicit file, actor, reason, request-id and --yes; it
compares the original committed dispatch, validates the participant and requeues
only the pending handler on its direct route. Completed/superseded/cancelled work
is not reopened. Operator authorization is hardened separately in phase 4.
