from helpers import write

write('Makefile', '''
.PHONY: check test integration native-check lint typecheck format format-check build production-gate
check: format-check lint typecheck test
lint:
\truff check src tests examples scripts
format:
\truff format src tests examples scripts
format-check:
\truff format --check src tests examples scripts
typecheck:
\tmypy src/duraflow
test:
\tPYTHONPATH=src:. pytest -m 'not integration' --cov=duraflow --cov-report=term-missing --junitxml=test-results.xml
integration:
\tPYTHONPATH=src:. pytest -m integration -v
native-check:
\tDURAFLOW_REQUIRE_NATIVE=1 PYTHONPATH=src:. pytest -v --cov=duraflow --cov-report=term-missing --cov-report=xml --cov-report=json --junitxml=native-results.xml
production-gate:
\tpython scripts/production_gate.py coverage.json
build:
\tpython -m build
''')
write('tests/conftest.py', '''
"""Required native validation must not silently succeed through skips."""
import os
import pytest


def pytest_sessionstart(session):
    session.native_count = 0
    session.skipped_reports = 0
    if os.environ.get("DURAFLOW_REQUIRE_NATIVE") == "1":
        required = ("DURAFLOW_TEST_POSTGRES", "DURAFLOW_TEST_PULSAR")
        if any(not os.environ.get(key) for key in required):
            raise pytest.UsageError("Required PostgreSQL/Pulsar endpoints are not configured")


def pytest_collection_modifyitems(session, items):
    session.native_count = sum(item.get_closest_marker("integration") is not None for item in items)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    report = (yield).get_result()
    if report.skipped:
        item.session.skipped_reports += 1


def pytest_sessionfinish(session, exitstatus):
    if os.environ.get("DURAFLOW_REQUIRE_NATIVE") == "1":
        if session.native_count < 2 or session.skipped_reports:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED
''')
write('scripts/production_gate.py', '''
"""Fail-closed coverage gate, separate from intermediate development checks."""
import json
import sys
from pathlib import Path

CRITICAL = ("coordinator.py", "state.py", "replay.py", "runner.py")


def check(data):
    failures = []
    totals = data["totals"]
    if totals["percent_covered"] < 80:
        failures.append("Overall branch-aware coverage must be at least 80%")
    for filename in CRITICAL:
        matches = [v["summary"] for k, v in data["files"].items() if k.endswith("/" + filename)]
        if len(matches) != 1 or not matches[0].get("num_branches"):
            failures.append(f"Missing critical branch evidence: {filename}")
        elif 100 * matches[0]["covered_branches"] / matches[0]["num_branches"] < 95:
            failures.append(f"Critical branch coverage must be at least 95%: {filename}")
    return failures


if __name__ == "__main__":
    failures = check(json.loads(Path(sys.argv[1]).read_text()))
    if failures:
        raise SystemExit("\\n".join(failures))
    print("Coverage gate passed; native failure, soak and pilot evidence are separate gates")
''')
write('docs/hardening/baseline.md', '''
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
''')
