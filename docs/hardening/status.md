# Production hardening status

Published baseline: PyPI `duraflow==0.1.0a1`, source f360e1d.
The changes below are unreleased source, not a production/PyPI deployment.

| Phase | Status | Evidence |
|---|---|---|
| 1: execution correctness | Implemented; functional validation passed | Staged CI 37543699908; main CI 37544034956 |
| 2: storage and scheduling | Implemented; functional validation passed | Staged CI 37545190528; main source 2a2e11a |
| 3: operational tooling | Implemented; functional validation passed | Staged CI 37546502779 and 37546744683; main source 43e7bd2 |
| 4: security and retention | Implemented; functional validation passed | Staged CI 37548592387; verified source cb44d62749280f1f824c094fc2187bc6cbb0a52a |
| 5: native failure qualification | Not complete | None |
| 6: candidate qualification | Not complete | None |

Phase 4 ran format/lint/type checks, all non-integration tests, all required
native PostgreSQL/Pulsar tests including actual restricted PostgreSQL accounts,
codec matrix, and wheel/sdist metadata validation before promotion. An invalid
archive request retains its original ValueError contract; deployment-policy
violations raise Conflict. Existing regression tests were not weakened.

TLS policy checks and native SQL role tests do not certify a user's installed
certificates, broker ACLs or production secret distribution. Runtime writers
remain trusted: Python policy is not an isolation boundary against direct SQL.

Migration from schema v1 to v2 requires stopping old writers; no mixed old/new
runtime write compatibility is claimed. Exact production release gates remain
open: complete fault/PITR matrix, measured load limits, real 24-hour soak,
critical coverage, deployment-specific authorization and a low-risk pilot.
