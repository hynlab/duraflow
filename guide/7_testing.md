# 7. Testing and contributing

[Index](0_index.md)

## Development checks

From a checkout with Python 3.12+ activated:

```bash
python -m pip install -e '.[dev,postgres,pulsar]'
make check
```

`make check` runs formatter checks, Ruff, mypy, and non-integration tests. CI also
tests a minimal `.[dev]` installation. Choose an interpreter with
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
Existing physical restore evidence exercises the legacy runtime; it is not evidence
of a protocol-2 database rollback/reconciliation qualification.

JUnit and coverage reports are produced by the Makefile. The established coverage
gate is distinct from behavioral fault tests. Do not infer new-runtime production
qualification solely from an earlier coverage report.

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
