# Complete local test environment

This Compose project builds the current checkout and runs PostgreSQL, Apache
Pulsar, a workflow engine, a workflow worker, and a task worker. Connection
environment variables are already set in `compose.yaml`; no `.env` or manual
`export` is needed. Requires Docker with Compose v2 and enough memory for Pulsar
(allocate at least 4 GiB to Docker).

Run from the repository root:

```bash
docker compose -f docker-compose/compose.yaml up --build -d --wait --wait-timeout 300
docker compose -f docker-compose/compose.yaml run --rm client
```

The client prints **22**, produced by the workflow in `examples/quickstart.py`
through the three independent services. `examples/runtime.py` loads the configured
environment, initializes each stateful role's journal, and manages its connections
using the public `Runtime` API. The client is an on-demand `test` profile service;
ordinary `up` starts the five long-lived containers only.

## Automatically configured connections

| Environment variable | Container value |
| --- | --- |
| `DURAFLOW_DATABASE_URL` | `postgresql+psycopg://duraflow:development-only@postgres:5432/duraflow` |
| `DURAFLOW_TASK_JOURNAL_URL` | Same PostgreSQL server; separate `duraflow_messages_tasks` schema |
| `DURAFLOW_PULSAR_URL` | `pulsar://pulsar:6650` |
| `DURAFLOW_NAMESPACE` | `compose-demo` |
| `DURAFLOW_PULSAR_TENANT` | `public` |
| `DURAFLOW_PULSAR_NAMESPACE` | `default` |
| `DURAFLOW_MESSAGE_SCHEMA` | `duraflow_messages` |
| `DURAFLOW_PROBE_HOST` / `DURAFLOW_PROBE_PORT` | `0.0.0.0` / `8090`, inside each service container |

The workflow worker and client omit DB credentials. Container service names are
used for networking, and Pulsar advertises `pulsar`, so every container can reach
the broker's returned address. No host ports are required or published. These
are disposable local development credentials, not a production configuration.

The client uses the stable request ID `runtime-demo-1`. Running it again returns
the recorded result. To create a fresh routing scope without deleting old data:

```bash
DURAFLOW_DEMO_NAMESPACE=another-demo docker compose -f docker-compose/compose.yaml up -d --wait
DURAFLOW_DEMO_NAMESPACE=another-demo docker compose -f docker-compose/compose.yaml run --rm client
```

## Inspect, restart, and clean up

```bash
docker compose -f docker-compose/compose.yaml ps
docker compose -f docker-compose/compose.yaml logs --tail=100 workflow-engine task-worker
docker compose -f docker-compose/compose.yaml exec workflow-engine python -m duraflow list

# Persisted broker messages and journals survive a service restart.
docker compose -f docker-compose/compose.yaml restart workflow-engine
docker compose -f docker-compose/compose.yaml run --rm client

# Stop, preserving data for the next run.
docker compose -f docker-compose/compose.yaml down

# Remove this demo's PostgreSQL/Pulsar volumes as well.
docker compose -f docker-compose/compose.yaml down -v
```

The root `compose.yaml` is a separate infrastructure-only project for host-based
development and native tests. Use that file and the
[distributed guide](../guide/5_distributed.md) when running Python on the host.
For SQLite, see the [Python example](../README.md#configure-in-python).
