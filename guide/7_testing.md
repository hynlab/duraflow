# 7. Testing and contributing

[Index](0_index.md)

## Development checks

From a checkout with Python 3.12+ activated:

```bash
python -m pip install -e '.[dev]'
make check
```

`make check` runs formatter checks, Ruff, mypy, and non-integration tests. CI also
tests the base dependencies plus `.[dev]` tools. Choose an interpreter with
`make check PYTHON=/path/to/python`.

| Command | Purpose |
| --- | --- |
| `make format` | Format source, tests, examples, and scripts |
| `make lint` / `make typecheck` | Ruff / mypy |
| `make test` | Non-integration tests and coverage |
| `make codec-check` | Frozen codec fixtures in supported dependency environments |
| `make native-check` | Required real-service tests, including protocol-2 process recovery |
| `make native-fault-check` | Explicitly guarded destructive Compose service tests |
| `make qualification-check` | Native checks, destructive tests, and coverage gate |
| `make message-soak-check` | 30-minute real-service workload with rolling role failures |
| `make extended-check` | Quality, native/fault qualification, sustained workload, and build |
| `make build` | Source and wheel distributions |

## Local message-driven tests

```python
from duraflow import Registry
from duraflow.testing import TestEnvironment
from examples.quickstart import double, example


async def test_example():
    async with TestEnvironment(Registry(example, double)) as env:
        handle = await env.client.start(example, 5, request_id="test-example")
        assert await env.run(handle) == 22
        assert (await handle.describe())["status"] == "COMPLETED"
```

Configure pytest-asyncio's `asyncio_mode = "auto"` or mark async tests explicitly.
The test environment runs the same consumers, state transitions, and outbox relays
as production, through a `MemoryBroker`. It does not directly advance the engine.

For waits:

1. `await env.drain()` to settle the current message traffic.
2. Send a signal, or use `env.clock.advance(seconds)` for virtual time.
3. `await env.run(handle)` to obtain the result.

For persistent local state, share the clock explicitly:

```python
from duraflow import ManualClock, SQLiteMessageStore

clock = ManualClock()
store = SQLiteMessageStore("test-state.db", clock=clock)
async with TestEnvironment(registry, store=store, clock=clock) as env:
    handle = await env.client.start(flow, value, request_id="persistent-test")
```

Here `registry`, `flow`, and `value` are your application. Task journals use a
separate store supplied through `journal=`. Virtual time controls orchestration
deadlines; it does not change arbitrary time inside user task functions.

## Native integration

Create a disposable local project, selecting unused ports:

```bash
export COMPOSE_PROJECT_NAME=duraflow-qualification-dev
export DURAFLOW_POSTGRES_PORT=54332
export DURAFLOW_PULSAR_PORT=56650
export DURAFLOW_PULSAR_ADMIN_PORT=58080
export DURAFLOW_TEST_POSTGRES='postgresql+psycopg://duraflow:development-only@localhost:54332/duraflow'
export DURAFLOW_TEST_PULSAR='pulsar://localhost:56650'
docker compose up -d --wait --wait-timeout 240
make native-check
```

`DURAFLOW_TEST_*` selects test infrastructure; application processes use the role
variables described in [operations](6_operations.md). Codec checks also need
package-index access for their temporary environments.

### Protocol-2 verification

- `tests/test_runtime_configuration.py`: code/environment precedence, secret-file
  selection, connection cleanup, SQLite journal contention, and public runtime recovery.
- `tests/test_native_runtime_configuration.py`: code and environment configuration
  against SQLite/Pulsar and PostgreSQL/Pulsar, with engine reconnection.
- `tests/test_message_runtime.py`: reception registration, filters, repeated
  signals, timeouts, tags, duplicate/stale events, outbox atomicity, and lifecycle.
- `tests/test_native_messages.py`: independent Pulsar clients and PostgreSQL
  journals, two state engines, real delayed delivery, and a broker-only CLI.
- `tests/test_message_process_recovery.py`: fresh-process SIGKILL at before-effect,
  after-effect, and after-result/before-ACK boundaries, plus both state engines
  killed during a signal wait. An independent SQLite effect ledger verifies the
  distinction between repeated invocations and one accepted external effect.

These process tests stop only their own subprocesses and drop their isolated
fixture schemas. They are part of `native-check`.

### Destructive service qualification

The separate fault suite interrupts disposable Compose PostgreSQL/Pulsar services
and exercises physical WAL restore. Explicitly opt in before running it:

```bash
export DURAFLOW_ALLOW_DESTRUCTIVE_TESTS=isolated-compose
make qualification-check
```

The guard validates project naming, container ownership, service identity, and
loopback port bindings. Required-native mode rejects missing endpoints and skips.
`tests/test_message_pitr.py` also exercises protocol-2 physical WAL restore with
both a restored and a newer task journal. It explicitly reissues an acknowledged
signal after restoring the workflow journal; this is **operator-assisted command
replay**, not automatic reconciliation of arbitrarily rolled-back databases with
advanced broker cursors. The broker and the external effect ledger remain current.

JUnit and coverage reports are produced by the Makefile. The established coverage
gate is distinct from behavioral fault tests. Do not infer new-runtime production
qualification solely from an earlier coverage report.

### Expanded protocol-2 qualification

See [the scenario catalog](../docs/qualification.md) for assertions, reproduction
commands, and limits. The additional suites cover:

- The same inbox/state/outbox contract on Memory, SQLite, and PostgreSQL, including
  concurrent connections, identity collisions, type distinctions, and disk quota failure.
- Publication/ACK/DLQ failures, stale owners, malformed envelopes, replay divergence,
  task deadlines, child cancellation, delegation, and paginated task-tag recovery.
- Hypothesis state machines (40 histories × up to 60 actions per local backend).
- Two independent engines and replay workers, 1/2/4 task workers, duplicate requests,
  process commit boundaries, graceful role replacement, and terminal-state races.
- A real TCP proxy that partitions an engine's database link; broker restart/pause
  and topic unload are exercised against guarded, owned Compose infrastructure.

The Makefile runs pytest through `scripts/pytest_guard.py`. Tests have a 180-second
watchdog (300 seconds for physical restore); the outer supervisor cleans registered
process groups, paused/stopped services, and interrupted PITR resources after a hard
pytest exit. Direct `python -m pytest` is useful for unit development; use the guarded
commands for native destructive qualification.

With the isolated endpoints above still configured, run the sustained profile:

```bash
make message-soak-check                    # SOAK_SECONDS=1800 by default
make message-soak-check SOAK_SECONDS=3600   # one hour
make extended-check                       # includes the destructive opt-in above
```

It asserts completed results, independent accepted effects, stable call counts for
settled-work restarts, and an empty outbox at the end. `soak-results.json` reports
batch latency percentiles, restart events, and aggregate **direct role-process** RSS;
RSS excludes replay children and server containers and is not a memory-leak proof.
`task-tags` is restarted as an idle role in this workload; its cancellation/fan-out
behavior has separate behavioral tests. Logs go to a unique directory under
`qualification-artifacts/`. A manual GitHub workflow runs the same sustained profile.

Preserve JUnit, coverage, and soak reports with the tested revision before another
run overwrites the top-level reports. WAL/base backups are intentionally excluded
from report artifacts and are removed from the disposable primary after PITR.

Remove the disposable environment after testing:

```bash
docker compose down -v
```

## Build and contribute

```bash
python -m pip install 'twine>=6.1,<8'
make build
python -m twine check --strict dist/*
```

Keep changes focused, add behavioral regressions for runtime fixes, run relevant
native tests for messaging or storage changes, and update the numbered guides.
Submit a pull request describing the change and actual checks run. Bug reports
should include reproducible synthetic inputs with credentials removed.
