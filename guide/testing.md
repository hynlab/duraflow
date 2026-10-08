# Testing and contributing

[Guide index](README.md)

## Set up a development environment

From a checkout with Python 3.12+ activated:

```bash
python -m pip install -e '.[dev,postgres,pulsar]'
make check
```

`make check` runs formatting checks, Ruff lint, mypy, and tests that do not require
native services. The CI unit jobs also test a minimal `.[dev]` installation.

| Command | Purpose |
| --- | --- |
| `make format` | Format Python source, tests, examples, and scripts |
| `make lint` | Run Ruff lint |
| `make typecheck` | Type-check the package |
| `make test` | Run non-integration tests with coverage |
| `make codec-check` | Check frozen fixtures against supported codec versions |
| `make native-check` | Run codec checks and all non-fault tests with real services |
| `make native-fault-check` | Run explicitly guarded process/service failure tests |
| `make qualification-check` | Run native checks, fault tests, and the coverage gate |
| `make build` | Build source and wheel distributions |

Use `make check PYTHON=/path/to/python` to choose an interpreter explicitly.

## Test your workflows locally

`TestEnvironment` drives a registry with an in-memory store and transport:

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

The repository configures pytest-asyncio's automatic mode. In your own project,
configure `asyncio_mode = "auto"` or mark async tests with `pytest.mark.asyncio`.

For workflows waiting on time or external input:

1. `await env.drain()` to reach the waiting state.
2. Advance simulated time with `env.clock.advance(seconds)` or send a signal.
3. Call `await env.run(handle)` to process the next steps.

For file-backed tests, pass `store=SQLiteStore(path)` to the environment.
The harness controls orchestration time, not real network delays or arbitrary
time spent inside task functions.

## Native integration tests

Create a dedicated local test environment. These alternate ports avoid the
default development service ports:

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

Choose unused ports if these are already occupied. Test connection variables
use `DURAFLOW_TEST_*`; CLI connection variables use `DURAFLOW_DATABASE_URL` and
`DURAFLOW_PULSAR_URL`.

The required-native mode rejects missing endpoints and skipped tests. Codec
checks create temporary environments and install the selected Pydantic versions,
so they need package-index access.

## Fault and recovery qualification

The fault suite kills worker/engine processes, interrupts PostgreSQL and Pulsar,
and restores an isolated physical PostgreSQL backup. Use only disposable
qualification services with the project and endpoint settings above.

```bash
export DURAFLOW_ALLOW_DESTRUCTIVE_TESTS=isolated-compose
make qualification-check
```

The guard checks the `duraflow-qualification-` project prefix, Compose ownership,
service identity, and loopback port bindings before interruption. Do not point
the tests at shared application data.

The suite produces JUnit XML, coverage reports, and
`qualification-results.json`. The coverage gate requires at least 80% overall
branch-aware coverage and 95% branch coverage in coordinator, state, replay, and
runner modules. Passing coverage is distinct from passing fault/recovery tests.

After testing, remove the disposable environment and its volumes:

```bash
docker compose down -v
```

## Build and check distributions

```bash
python -m pip install 'twine>=6.1,<8'
make build
python -m twine check --strict dist/*
```

## Contribute a change

1. Open an issue for a bug or a substantial design change, with a reproducible case.
2. Keep the change focused and follow the surrounding Python style.
3. Add behavioral regression coverage for runtime fixes.
4. Run `make check`; run native checks when changing storage or transport behavior.
5. Update the relevant guide when user-facing behavior changes.
6. Submit a pull request explaining the change and the checks you ran.

Use synthetic inputs in reports and remove credentials and private payloads.
The project uses the [Apache License 2.0](../LICENSE).
