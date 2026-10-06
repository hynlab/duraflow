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
