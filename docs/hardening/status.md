# Production hardening status

Published baseline: PyPI `duraflow==0.1.0a1`, source f360e1d.
These are unreleased repository changes. PyPI is not modified by these commits.

| Phase | Status | Executed evidence |
|---|---|---|
| 1: execution correctness | Implemented, functional checks passed | Staged CI 37543699908; main CI 37544034956 |
| 2: storage and due scheduling | Implemented, functional checks passed | Staged CI 37545190528, source 7ccd0b292511293203b18e9d3e83399df4f64f70 |
| 3: operational tooling | Not complete | None |
| 4: security and retention | Not complete | None |
| 5: native failure qualification | Not complete | None |
| 6: candidate qualification | Not complete | None |

Phase 1 covers A01-A06; see phase1.md for replay, lifecycle and codec boundaries.
Phase 2 covers B01-B04: rollback-safe migrations, row-locked server-time mutations,
indexed due-run selection and bounded infrastructure retry. Full existing tests,
new native PostgreSQL tests, actual Pulsar integration, format/lint/mypy, frozen
codec matrices and package validation passed before promotion.

Schema v1 -> v2 requires a maintenance window with old runtime writers stopped.
It is not an online mixed-alpha/new-runtime schema upgrade. Logical workflow
build coexistence on the new runtime remains supported.

Functional success is not production approval. Coverage thresholds, native
crash/PITR matrices, realistic load, actual 24-hour soak and user-environment
pilot remain explicit independent gates. No production deployment is asserted.
