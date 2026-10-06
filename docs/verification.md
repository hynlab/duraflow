# Executed verification evidence

## Passing source revision

Source commit: `70e82c38cfe0a78ab025dcba94b89a88a5d55004`.
CI run: https://github.com/hynlab/duraflow/actions/runs/37522053193

All four jobs completed successfully:

| Job/step | Observed result |
|---|---|
| Unit suite, Python 3.12 | Passed |
| Unit suite, Python 3.13 | Passed |
| Both examples | Passed on both Python versions |
| Wheel/sdist build and Apache-2.0/typed-marker inspection | Passed on both Python versions |
| Ruff lint and mypy | Passed |
| PostgreSQL and Pulsar native integration | Passed |

The native job starts actual PostgreSQL 16 and Pulsar 4.0.3 containers, exercises
durable start/CAS/replay with the PostgreSQL adapter and the three-subscription
broadcast using separate engine/worker broker connections, then tears down the
containers. It is not a mock-only adapter test. Hypothesis is installed in CI.

The first two CI runs exposed ambiguous generic variable names and one CLI
callable-type inference error. Both were fixed in source; quality checks were
not removed, ignored or weakened to make this run pass.

## Local isolated environment

Python 3.13.5, Pydantic 2.13.4: **57 passed, 3 skipped**. The skipped items are the
two tests requiring native services and the optional Hypothesis module. The
local measured all-module branch-aware coverage is approximately 77%; native
adapter paths are not exercised in that local result. Both examples, CLI help,
and wheel/sdist metadata checks were also executed.

A subprocess test hard-exits after two of three broadcast handlers finish,
restores a file-backed database backup, starts a fresh process, and verifies
that only the unfinished handler executes and one final logical publication is
produced. This does not substitute for a real PostgreSQL/Pulsar crash/PITR matrix.

## Remaining release gates

Passing functional checks do not close production gates for real-service
crash/PITR matrices, 24-hour soak/load, rolling upgrades, coverage targets,
complete telemetry or authorization review. The alpha is installable and tested,
not asserted to be production-qualified. See
[implementation_status.md](implementation_status.md).
