# A01 acceptance baseline

Baseline: f360e1d1dd4c1f809bb88fa8c6ddeb608129fb7f, published alpha 0.1.0a1.
Scope: trusted internal PostgreSQL/Pulsar deployment, not public multi-tenancy.
The existing test suite is retained. This mechanical formatting commit establishes
the Ruff formatting baseline before lifecycle implementation.

Shared commands: make check; make native-check; python -m build; twine check.
Required native validation errors on absent endpoints, fewer than two native
cases, or skipped test reports. It is not acceptable to claim a skipped service
was exercised. CI/publishing wiring is reviewed separately; this application
change does not modify workflow permissions or workflow files.

The candidate coverage report is an artifact, not an assertion that the production
gate passed. make production-gate requires 80% overall branch-aware coverage and
95% branches in coordinator.py, state.py, replay.py and runner.py. Failure/crash,
restore, real 24-hour soak and pilot gates remain independent.

Supported validation baseline: Python 3.12/3.13; PostgreSQL 16; Pulsar 4.0.3.
No production deployment or new PyPI publication is performed by this change.
