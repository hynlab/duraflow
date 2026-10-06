# Production hardening status

Published baseline: PyPI `duraflow==0.1.0a1`, source f360e1d.
This branch contains unreleased changes; PyPI is not modified by these commits.

## Phase 1 — implemented and functionally verified

A01 validation baseline and fail-closed native checks; A02 replay subprocess
isolation; A03 cancellation after worker loss; A04 versioned acceptance-time
deadlines; A05 exact-build execution routing; A06 pinned codec fixtures and
Pydantic patch-version matrix.

Evidence: staged CI run 37543699908, verified source
fb351f02f9e333843c84a7514f476a443c117f58. Shared format/lint/mypy/unit checks,
actual PostgreSQL/Pulsar integration, codec matrix and distribution checks passed.
See phase1.md for compatibility restrictions and the role of B02 authoritative time.

## Remaining

Phases 2–6 are not yet complete. The separate production coverage gate, actual
native fault/restore/load qualification, real 24-hour soak and user-environment
pilot have not been certified. Functional CI success is not production approval.
