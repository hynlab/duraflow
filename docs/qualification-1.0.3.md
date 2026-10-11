# 1.0.3 release verification

## Scope and decisions

The review bundle `/Users/hyun/capybara/duraflow_deep_review_2026-10-11`
describes nine reproducible findings against main `3c6eb4d`. The implementation
keeps the state-owner/replay-worker/task-worker architecture and repairs its
control, deadline, snapshot, callback, identity, and telemetry boundaries.
The user explicitly approved including the pre-existing lifecycle changes.

The original probes assert defective behavior. They were read as evidence and
converted into positive regressions in `tests/test_deep_review_regressions.py`;
the original review bundle was not modified.

## Review findings addressed before release

- Keep output-size errors separate from malformed inputs; commit the accepted
  observation and suppress only the oversized activation. Supply a bounded
  diagnostic summary and verify audited resume reuses successful work.
- Recheck deadline/ownership after a potentially slow publication, immediately
  before invocation. Include deadline fields in task identity fencing and retain
  timeout error codes for retry policy evaluation.
- Keep execution/control route cursors independent; verify external completion
  as well as cancellation while all execution slots are occupied.
- Explicitly reject empty as well as nonexistent archive targets.
- Backfill narrow telemetry columns during transactional migration. No repeated
  full JSON-history scan in health probes; unavailable metrics render `NaN`.
- Verify migrations from actual old table layouts, not only old version markers.

The review covered correctness, readability, architecture, input/identity
validation, and performance. Snapshot-budget handling was extracted into
`workflow_snapshot.py`; telemetry projection helpers live in
`message_telemetry.py`. No new package dependencies were added.

## Qualification results

| Check | Final local result |
| --- | --- |
| Formatting, Ruff, mypy | Passed; 44 source modules type-checked |
| Frozen codec matrix | Passed for Pydantic 2.13.4 and 2.13.5 |
| `make native-check` | 448 passed, 0 failed, 0 skipped (246.26 seconds) |
| `make native-fault-check` | 16 passed, 0 failed, 0 skipped (209.83 seconds) |
| `make production-gate` | Passed after the combined native/fault run |
| Build and strict Twine checks | Wheel and sdist passed |
| Isolated installed-wheel smoke | 1.0.3 metadata/imports and dependency check passed outside the source checkout |
| Quickstart and broadcast/join examples | Passed (`22`; final payload `['27']`) |
| Real-service lifecycle soak | Passed; details below |

The native and fault suites cover **464 distinct tests**. The new review
regression file contributes 39 cases; two additional PostgreSQL migration cases
cover previous table layouts. The release workflow repeats qualification on the
tagged Linux checkout before publishing and verifies the published artifact
hashes and installation independently.

The final 180-second requested lifecycle soak completed in **182.688 seconds**:
49 continuation generations, 147 external effects, four role-loop restarts,
maximum hot state 3,405 bytes, producer/declaration caches bounded at 32 entries,
and drained outboxes. The local raw output is
`/var/folders/mt/8_2_100d2kvcg520rlnj75_00000gn/T/opencode/duraflow-lifecycle-103.json`.

Local JUnit/coverage evidence is in `native-results.xml`, `fault-results.xml`,
`coverage.json`, and `coverage.xml`. Published artifacts and Linux qualification
logs are retained by the existing GitHub release workflow.

## Environment

Local validation uses Python 3.12.15 and isolated Compose project
`duraflow-qualification-103`: PostgreSQL 16 and Pulsar 4.0.3 on loopback ports
55432/16650/18080. Native fault tests verify resource ownership before service
kill/pause or physical restore. GitHub release qualification runs independently
on Linux and publishes through the existing PyPI Trusted Publishing workflow.

Short soak and automated qualification do not constitute a 72-hour soak or a
multi-host production workload qualification. Historical lifecycle evidence in
`qualification-lifecycle-2026-10-11.md` predates the additional review fixes.
