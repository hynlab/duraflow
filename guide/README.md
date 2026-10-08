# Duraflow guides

These guides describe the source on `main`. An earlier installed release may
not include every feature shown here.

## Start here

1. [Getting started](getting-started.md) — installation and a local workflow.
2. [Writing workflows](workflows.md) — durable operations and task execution.
3. [Broadcast and join](broadcast-and-join.md) — event-driven fan-out and joins.
4. [Running distributed services](distributed.md) — PostgreSQL/Pulsar setup and CLI.
5. [Operations](operations.md) — configuration, monitoring, and recovery.
6. [Testing and contributing](testing.md) — local tests and repository development.

## Key concepts

| Concept | Meaning |
| --- | --- |
| Workflow | An async function describing durable orchestration |
| Task | A sync or async function executed by a worker |
| Registry | The workflow and task implementations available to a process |
| Engine | Replays workflows and schedules their next operations |
| Worker | Claims task deliveries, executes functions, and records results |
| Run | One workflow execution history, identified by `run_id` |
| Workflow ID | A logical identity that can span runs through rollover |
| Request ID | An idempotency key for starting a workflow or issuing a control |
| Namespace | A scope for workflow identities and internal message routing |

Historical design notes and verification reports live in [`docs/`](../docs/).
For hands-on usage, begin with these guides.

[Back to the project](../README.md)
