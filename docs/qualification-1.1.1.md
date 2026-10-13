# 1.1.1 release verification

## Local checks (2026-10-12)

- macOS arm64, Python 3.12.15, pytest 9.1.1, Pydantic 2.13.5,
  SQLAlchemy 2.0.54, Hypothesis 6.169.0.
- The unchanged 1.1.0 source passed **394 existing non-integration tests**. The
  independently reconstructed audit suite reproduced all four reported defects:
  **11 failed, 4 passed** before the fixes.
- After the fixes, `make check` passed format, Ruff lint, mypy (45 source files),
  and **437 tests**, with **83 native integration/fault tests deselected** and
  no skips or failures. The test run took 86.72 seconds locally.
- The new suite now contains **43 passing regression/control cases**, covering:
  - 3- and 300-member failing fan-outs with inline and actual subprocess replay;
  - the 256 KiB boundary at minus one byte, exactly the limit, and plus one byte;
  - large Unicode metadata and individually valid, deeply nested errors;
  - replay of pre-upgrade oversized histories, preserving their stored content;
  - strict individual task error validation;
  - malformed workflow names and instance IDs, plus a transient store failure
    that must remain retryable;
  - extreme and ordinary retry policies on Memory and SQLite, slow growth after
    attempt 31, and high-precision reference calculations for tiny/large values.
- `failures.py` and `transitions.py` reached 100% statement/branch coverage in
  this local run. This is not a claim of full-system/native coverage.
- Frozen-codec checks passed on both Pydantic 2.13.4 and 2.13.5.
- Quickstart returned `22`; broadcast/join returned `['27']`.
- Wheel and sdist built successfully; strict Twine checks passed. An isolated
  wheel installation outside the checkout verified version 1.1.1, native adapter
  imports, dependency compatibility, the new bounded-error helper, and CLI startup.
- Code review covered correctness, readability, architecture, security, and
  performance. Size/depth limits are preserved, database errors are not converted
  to protocol errors, retries remain bounded by the public attempt limit, and
  replay recovery does not rewrite committed history.

## Docker files and native-service verification

All tracked Docker files have active consumers:

- Root `compose.yaml`: native tests, qualification/fault tests, and publishing gates.
- `docker-compose/compose.yaml` and `Dockerfile`: the documented distributed demo
  and the CI `compose-demo` job.
- `Dockerfile.dockerignore`: excludes Python bytecode/cache and package metadata
  from that build's copied source trees.

No unused Docker files were found to delete. Local services could not be started:
Docker Hub returned an authentication error, and the public ECR PostgreSQL mirror
returned a rate-limit error. No native-service success is inferred from these
attempts or from older CI runs. The commit's CI must supply fresh PostgreSQL/Pulsar,
fault-injection, and Compose-demo results.

Local audit logs/distributions remain in the ignored `audit-results/` directory;
the regression code is tracked in `tests/test_audit_report_regressions.py` and
`tests/audit_app.py`. Version metadata and release notes do not themselves imply
that the package has been published to PyPI.
