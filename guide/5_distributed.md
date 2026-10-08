# 5. Running distributed services

[Index](0_index.md) · Previous: [Broadcast and join](4_broadcast_and_join.md)

Run commands from the repository root with your Python environment activated.
This example uses PostgreSQL 16, Pulsar 4.0.3, and the approval application.

## 1. Install adapters and start infrastructure

```bash
python -m pip install -e '.[postgres,pulsar]'
docker compose up -d --wait --wait-timeout 240
```

Compose uses loopback bindings, development credentials, and a Pulsar broker
advertised as `localhost` for host processes. If the default ports are occupied,
set alternatives before starting:

```bash
export DURAFLOW_POSTGRES_PORT=54332
export DURAFLOW_PULSAR_PORT=56650
export DURAFLOW_PULSAR_ADMIN_PORT=58080
docker compose up -d --wait --wait-timeout 240
```

Use unused ports. Containerized application workers need an advertised broker
address reachable from their own network.

## 2. Configure and initialize journals

```bash
export DURAFLOW_DATABASE_URL="postgresql+psycopg://duraflow:development-only@localhost:${DURAFLOW_POSTGRES_PORT:-5432}/duraflow"
export DURAFLOW_TASK_JOURNAL_URL="$DURAFLOW_DATABASE_URL"
export DURAFLOW_PULSAR_URL="pulsar://localhost:${DURAFLOW_PULSAR_PORT:-6650}"
export DURAFLOW_NAMESPACE=demo

python -m duraflow init
python -m duraflow --database "$DURAFLOW_TASK_JOURNAL_URL" --schema duraflow_messages_tasks init
```

The first journal is workflow state (`duraflow_messages`); the second is a separate
service execution schema (`duraflow_messages_tasks`). Sharing a local PostgreSQL
server is convenient, but each role can use a different database and principal.

Export the applicable variables in each terminal. If using alternative ports,
copy the resolved URLs or export the same port variables there as well.

## 3. Start independent roles

Workflow state engine — DB and broker, no application implementation needed:

```bash
python -m duraflow --workflow approval-example --probe-port 8092 workflow-engine
```

Workflow worker — broker and importable orchestration application:

```bash
python -m duraflow --app examples.signals --probe-port 8093 workflow-worker
```

Task worker — broker and its independent execution journal:

```bash
python -m duraflow --app examples.signals --probe-port 8094 task-worker
```

Each command runs in its own terminal. Client and workflow-worker processes do
not need `DURAFLOW_DATABASE_URL`. Task workers use `DURAFLOW_TASK_JOURNAL_URL`,
not the workflow database URL. The workflow worker uses a bounded replay subprocess
pool and requires the application module to be importable by child processes.

If using workflow tags, also start:

```bash
python -m duraflow --probe-port 8095 tag-engine
```

For task tags, provision a separate tag journal and run `task-tag-engine` with
`--database` and `--schema` selecting that journal. Global options precede the command.

## 4. Start through a contracts-only client

```bash
python -m duraflow --app examples.signal_contracts start approval-example:v1 \
  --input '7' --request-id order-42
```

The response contains a logical `workflow_id` (`order-42`) and a `run_id` for its
history. Inspect the instance and wait until the `channels` field shows reception
registered before manually sending approval:

```bash
python -m duraflow --app examples.signal_contracts describe order-42 --include-payload
python -m duraflow --app examples.signal_contracts signal order-42 approval \
  --input 'true' --signal-id approval-42
python -m duraflow --app examples.signal_contracts result order-42 --timeout 30
```

Expected result: `14`. The CLI's instance argument is the **logical workflow ID**
for protocol 2. With an app containing several workflow contracts, select one
using `--workflow name:vN` before the command.

## 5. Stop and resume

Ctrl-C or SIGTERM requests bounded shutdown. Start the same roles with their
existing journal settings to resume pending work. Messages remain in Pulsar and
committed state remains in PostgreSQL.

```bash
docker compose down
```

This keeps volumes. `docker compose down -v` deletes them. Keep the same Compose
project and port configuration when restarting the environment.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Start accepted, no progress | Workflow worker is subscribed to the pinned build route |
| Task stays pending | Task worker, contract/version, journal health, and broker subscription |
| Approval did not resume the workflow | Reception was registered before the signal was accepted |
| Tagged start waits | `TagEngine` is running and has committed the tag registration |
| Store errors | Correct schema initialized; role's DB principal has journal permissions |
| Broker connection failure | Compose health, host ports, and advertised address |
| `BLOCKED` | Replay mismatch, limits, unsupported operations, or exhausted task policy |

Next: [Operations](6_operations.md).
