# 1.1.0 release verification

## Local checks (2026-10-11)

- macOS, Python 3.12.15, Pydantic 2.13.5.
- `make check`: format, Ruff lint, mypy (44 source files), and **394 tests passed**;
  83 native integration/fault tests were explicitly deselected, no skips/failures.
- Includes **13 new regressions** covering real 16 MiB boundaries on memory and
  SQLite with inline and real subprocess replay, the fitting control, exact wire
  limit and one-byte overflow, child-manifest expansion, lost publication
  confirmation/redelivery identity, blocked resume, and child reuse after overflow.
- The independent retest bundle's unchanged tests: **15 passed**, including the
  two formerly failing process-envelope cases and the fitting control.
- Code review found no required changes across correctness, architecture,
  readability, security, and performance.
- Wheel and sdist built successfully; strict Twine metadata checks passed. An
  isolated installed-wheel smoke outside the checkout verified 1.1.0 metadata,
  imports of native adapters, and dependency compatibility.

An initial local Python 3.13.3/Homebrew coverage run stalled in the pre-existing
SQLite concurrent-start test and was interrupted; it is not counted as a passing
suite. The complete verification above uses a fresh Python 3.12.15 environment.
CI separately tests Python 3.12 and 3.13.

## Release gates

Native-service and fault-injection results are produced by the new commit's CI
and the `v1.1.0` publishing workflow, not inferred from 1.0.3 results. The publishing
workflow runs `make qualification-check`, builds wheel/sdist, validates metadata,
smoke-tests the installed wheel, and publishes only after those gates pass. Its
final job verifies PyPI SHA256 values against the built artifacts and installs
exactly `duraflow==1.1.0` from PyPI. Check the release workflow conclusion for the
publication result.
