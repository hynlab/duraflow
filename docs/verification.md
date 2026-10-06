# Executed verification evidence

## Local isolated environment

Python 3.13.5, Pydantic 2.13.4: **57 passed, 3 skipped**. The skipped items are the
two tests requiring native services and the optional Hypothesis module. The
local measured all-module branch-aware coverage is approximately 77%; native
adapter paths are not exercised in that local result. Both examples, CLI help,
and wheel/sdist metadata checks were also executed.

## GitHub Actions native environment

Run: https://github.com/hynlab/duraflow/actions/runs/37521302178
Source commit: `27d61579ded4aca892be3f4e6075cbba7e259b85`.

| Job/step | Observed result |
|---|---|
| Unit suite, Python 3.12 | Passed |
| Unit suite, Python 3.13 | Passed |
| Examples, wheel/sdist build and license/typed-marker inspection | Passed on both Python versions |
| PostgreSQL and Pulsar native integration | Passed |
| Ruff lint | Passed |
| mypy | Found one CLI callable-type inference error; corrected in the subsequent commit |

The native job starts actual PostgreSQL 16 and Pulsar 4.0.3 containers, exercises
durable start/CAS/replay with the PostgreSQL adapter and the three-subscription
broadcast using separate engine/worker broker connections, then tears down the
containers. It is not a mock-only adapter test. Hypothesis is installed in CI.

The CI run linked above is historical evidence, not a claim that its overall
conclusion was green. Follow the repository's current CI check for the post-fix
quality result. No lint/typecheck failure is suppressed to make the build pass.

These passing functional checks do not close the production release gates for
real-service crash/PITR matrices, long-running soak/load, rolling upgrades,
coverage targets, complete telemetry or authorization review. See
[implementation_status.md](implementation_status.md).
