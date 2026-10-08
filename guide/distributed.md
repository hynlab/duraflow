# Running distributed services

[Guide index](README.md) · Previous: [Broadcast and join](broadcast-and-join.md)

This walkthrough runs engines and workers on your host, with PostgreSQL 16 and
Pulsar 4.0.3 in Docker Compose. Run commands from the repository root with your
Python environment activated.

## 1. Install adapters and start services

```bash
python -m pip install -e '.[postgres,pulsar]'
docker compose up -d --wait --wait-timeout 240
```

The Compose file binds services to loopback and uses development credentials.
It advertises Pulsar as `localhost`, matching clients running on the host.
Containerized application processes need their own reachable broker address.

Default host ports are `5432` (PostgreSQL), `6650` (Pulsar), and `8080` (Pulsar
admin). If they are occupied, set alternatives **before** starting Compose:

```bash
export DURAFLOW_POSTGRES_PORT=54332
export DURAFLOW_PULSAR_PORT=56650
export DURAFLOW_PULSAR_ADMIN_PORT=58080
docker compose up -d --wait --wait-timeout 240
```

## 2. Configure the application

Set these variables in every terminal used below:

```bash
export DURAFLOW_DATABASE_URL="postgresql+psycopg://duraflow:development-only@localhost:${DURAFLOW_POSTGRES_PORT:-5432}/duraflow"
export DURAFLOW_PULSAR_URL="pulsar://localhost:${DURAFLOW_PULSAR_PORT:-6650}"
export DURAFLOW_NAMESPACE=demo
```

If you selected alternative ports, export those port variables in each terminal
as well, or use the resolved URL values directly.

Initialize the database:

```bash
python -m duraflow --app examples.application init
```

The application module exports `registry` and, for broadcast consumers,
`broadcasts`. It must be importable by every process, including replay
subprocesses. See [`examples/application.py`](../examples/application.py).

## 3. Start an engine and a worker

In one terminal:

```bash
python -m duraflow --app examples.application --probe-port 8092 engine
```

In another terminal:

```bash
python -m duraflow --app examples.application --probe-port 8093 worker
```

The engine replays and schedules workflows. The worker executes the registered
tasks. You can run more processes against the same namespace and services;
workers claim execution leases and engine updates use revision checks.

Global CLI options, such as `--app`, `--namespace`, and `--probe-port`, go
**before** the subcommand.

## 4. Start and inspect a workflow

In a third terminal:

```bash
python -m duraflow --app examples.application start product:v1 \
  --input '7' --request-id product-demo-1
```

The command returns JSON containing `run_id`. Assign that value to `RUN_ID`:

```bash
RUN_ID='<run_id from the start response>'
python -m duraflow describe "$RUN_ID"
python -m duraflow history "$RUN_ID"
python -m duraflow attempts "$RUN_ID"
python -m duraflow list --limit 20
```

Once all three handlers finish, the workflow publishes `27` to the output topic
and becomes `COMPLETED`. The workflow result is the publication receipt. To
include the result and stored payloads in the response:

```bash
python -m duraflow describe "$RUN_ID" --include-payload
```

Repeating the start with identical parameters and request ID returns the same
execution. Use a new identity for a new example run.

## 5. Stop and restart

Use Ctrl-C or SIGTERM to stop the engine and worker. Start them again with the
same application and connection settings to resume pending work. PostgreSQL is
the source of workflow state; Compose named volumes preserve service data.

Stop the development services with:

```bash
docker compose down
```

Adding `-v` deletes their volumes and data. Keep the same Compose project name
and port settings when restarting an existing environment.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Run stays waiting | Engine and worker are running with the same namespace; required tasks are registered |
| `ModuleNotFoundError` | Run from the repository root or install the application's module |
| Port already allocated | Select alternative Compose ports and update both connection URLs |
| Broker connection fails | `docker compose ps`, broker health, and its advertised address |
| Run is `BLOCKED` | Inspect its `blocked_reason`, task attempts, and pinned implementation |

Use `docker compose logs postgres pulsar` for infrastructure diagnostics and
`python -m duraflow --help` for the available CLI commands.

Next: [Operations](operations.md).
