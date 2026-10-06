# Production hardening status

Published baseline: PyPI `duraflow==0.1.0a1`, source f360e1d.
These are unreleased source changes; no new PyPI or production deployment.

| Phase | Status | Executed evidence |
|---|---|---|
| 1: execution correctness | Implemented, functional checks passed | Staged CI 37543699908; main CI 37544034956 |
| 2: storage and due scheduling | Implemented, functional checks passed | Staged CI 37545190528; main CI 37545484522 |
| 3: operational tooling | Implemented, functional checks passed | Staged CI 37546502779; verified source 77a39c0c4a8b2b7134b68be98c610ff11ba388eb |
| 4: security and retention | Not complete | None |
| 5: native failure qualification | Not complete | None |
| 6: candidate qualification | Not complete | None |

Phase 1: A01-A06; replay isolation, lifecycle acceptance ordering, exact-build
routing and immutable codec fixtures. Phase 2: B01-B04; rollback-safe migrations,
row-locked server time, indexed outstanding obligations and bounded infra retry.
Phase 3: C01-C06; validated settings, process shutdown, read-only probes, safe
correlation logs, bounded metric labels, and validated handler-specific DLQ replay.
The phase-3 tests include a genuinely stuck synchronous task in a disposable
process and SIGTERM/hard-exit verification. Native PostgreSQL/Pulsar checks,
codec matrices, formatting/lint/types, unit tests and package validation passed.

Prometheus alert configuration is supplied; actual alert delivery and production
notification routing are not asserted. Scope remains trusted internal services.
Schema v1 -> v2 requires stopping old runtime writers for a maintenance migration.

Functional CI is not production approval. The separate coverage gate, complete
native crash/PITR matrices, measured workload limits, actual 24-hour soak and
user-environment pilot remain independent gates. See phase1.md–phase3.md.
