# 8. Internal architecture

[Index](0_index.md) · [Project overview](../README.md)

The current runtime uses Infinitic-style message-driven component boundaries.
Python functions, typed references, and an independently authored JSON protocol
implement those boundaries.

## Components and state ownership

```text
Client ── start / signal / query ──► command topic
                                        │
                                 WorkflowEngine
                                        │
                               atomic state + outbox
                                        │
                     ┌──────────────────┴──────────────────┐
                     ▼                                     ▼
              replay topic                           task topic
                     │                                     │
              WorkflowWorker                         TaskWorker
              replay snapshot                        business code
                     │                                     │
             activation_result                         task_result
                     └──────────────────┬──────────────────┘
                                        ▼
                                  workflow inbox
                                        │
                                next activation ...
                                        │
                                client response topic
                                        ▼
                                      Client
```

| Component | Owns | Does not require |
| --- | --- | --- |
| `Client` | Correlated reply subscription | Workflow DB or user implementation |
| `WorkflowEngine` | Workflow history, waits, active activation, buffered events | User workflow code |
| `WorkflowWorker` | Disposable replay process pool | Workflow DB |
| `TaskWorker` | Service-owned execution journal, leases, cached results | Workflow DB |
| `TagEngine` | Workflow tag memberships and fan-out snapshots | Workflow history |
| `TaskTagEngine` | Task tag memberships and control fan-out | Workflow history |

An outbox relay accompanies journal-owning roles. Roles can share an application
process, but storage and topic interfaces remain explicit. CLI deployments run
them independently.

## Topic topology

`Topics(namespace="demo", tenant="public", environment="default")` generates
topics below `persistent://public/default/df2-4-demo-`.

| Suffix | Purpose | Subscription |
| --- | --- | --- |
| `command-{name}` | Public start, query, signal, and result requests | `state`, Key_Shared |
| `workflow-{name}` | Internal activation/task/child results and routed signals | `state`, Key_Shared |
| `control-{name}` | Workflow operator commands | `state`, Key_Shared |
| `replay-{name}-{build_hash}` | Workflow activation snapshots | `replay`, Key_Shared |
| `task-{name}-v{version}` | Service execution | `tasks`, Shared |
| `task-completion-{name}-v{version}` | Token-authorized delegated completion | `tasks`, Shared |
| `task-control-{name}-v{version}` | Service cancellation | `tasks`, Shared |
| `timer-{name}` | Delayed timers and workflow deadlines | `timers`, Shared |
| `tags-workflows` | Workflow membership and signal fan-out | `tags`, Key_Shared |
| `task-tags-tasks` | Service tag membership | `tags`, Key_Shared |
| `task-tag-control-tasks` | Tagged service controls | `tags`, Key_Shared |
| `reply-{client_id}` | Correlated client responses | `client`, Shared |

Workflow keys encode namespace, workflow type, and logical ID. Service execution
keys identify one attempt. Build-specific replay routes keep old workers from
consuming a new implementation's activation. No topic is allocated per run.
Client reply subscriptions are cleaned up on close after pending requests finish.
Public command topics cannot inject executor events. Delegated-completion topics
cannot dispatch arbitrary service functions. Broker ACLs restrict each producer
role to its own command, event, execution, or operator topics.
Business topics are explicitly declared with `TopicRef`, with named participant
subscriptions and metadata in `duraflow-v2`.

## One logical execution

1. Client publishes `start`; synchronous `start()` waits for engine acceptance.
2. Engine creates state and commits `activate` to its outbox.
3. Workflow worker reconstructs the coroutine from the immutable snapshot.
4. Recorded commands are checked and completed results are injected.
5. Worker returns an ordered command batch or waiting/completed/failed decision.
6. Engine commits new commands and their outgoing execution/timer messages.
7. Task worker claims a journal lease, executes outside transactions, and commits
   its result with a `task_result` outbox entry before ACK.
8. Engine accepts the result, resolves its wait/join, and schedules another activation.
9. On completion, the engine replies to registered result waiters through Pulsar.

`ctx.channel(...).receive()`, `ctx.dispatch()`, and `ctx.timer()` register work
without suspending. The replay driver collects those registrations before the
next await. Registration and subsequent dispatch intent commit together.

## Transactional boundaries

Each journal stores:

```text
states:  key → JSON document
inbox:   (state key, message ID) → envelope digest
outbox:  message ID → publication + publisher lease
```

PostgreSQL serializes changes with a row lock on the logical state key. SQLite
uses a local write transaction; memory uses a lock. Input deduplication, state
changes, and outgoing messages commit atomically. User code and broker calls
never execute under a state transaction.

```text
receive → validate → transaction(inbox + state + outbox) → COMMIT → ACK
                               │
                       relay claims outbox
                               │
                       publish → delivery mark
```

The workflow aggregate includes run histories, active activation ID, buffered
events, start identities, and result waiters. The service journal is independently
configured and holds invocation digest, generation, lease, delegation, and result.
These journals may share a development server but do not share logical ownership.

## Ordering and concurrency

Key_Shared reduces concurrent processing of one workflow, but correctness also
depends on state locks and activation IDs. A result from an old activation cannot
append new commands. Input arriving during replay is persisted until that replay's
ordered commands commit; this prevents an event from overwriting its own registration.

Independent producers do not establish a global order. Accepted transitions are
recorded in history and replay uses that history. Duplicate message IDs with
different content are integrity conflicts. Matching duplicate outcomes do not
advance a join or signal receive twice.

Task journals fence physical owners by generation and lease. A lease takeover
does not create an application retry; an application retry has a new attempt
dispatch identity and delayed execution request.

## Timers, recovery, and external effects

Timer and retry reservations are delayed broker messages, not workflow scans.
`not_before` is checked against the receiving journal's clock. An early delivery
is NACKed without recording it as consumed, so it can be accepted later.

| Failure point | Recovery |
| --- | --- |
| Before input transaction commit | Broker redelivers; no partial state |
| After commit, before ACK | Inbox detects repeat; committed outbox survives |
| After publish, before delivery mark | Stable message may be published again |
| Workflow worker exits | Unacknowledged activation is replayed by its replacement |
| Task worker exits before result commit | Expired lease can be reclaimed; effect may repeat |
| Task result committed before ACK | Replacement reuses the journaled result |
| Both workflow engines exit during a signal wait | State persists and signal remains queued for replacement engines |

State-transition deduplication does not make external effects exactly-once.
Use the stable business idempotency key in downstream APIs or business databases.
Cancel/terminate cannot reverse completed effects or forcibly stop arbitrary threads.

## Source map

| File | Responsibility |
| --- | --- |
| [`client.py`](../src/duraflow/client.py) | Broker SDK and handles |
| [`messaging.py`](../src/duraflow/messaging.py) | Envelopes, publications, topology |
| [`workflow_engine.py`](../src/duraflow/workflow_engine.py) | Workflow state transitions |
| [`workflow_worker.py`](../src/duraflow/workflow_worker.py) | Snapshot execution consumer |
| [`workflow_replay.py`](../src/duraflow/workflow_replay.py) | Replay and non-blocking command registration |
| [`channels.py`](../src/duraflow/channels.py) | Typed reception and declarative filters |
| [`task_worker.py`](../src/duraflow/task_worker.py) | Service execution, ownership, delegation |
| [`tag_engine.py`](../src/duraflow/tag_engine.py) / [`task_tags.py`](../src/duraflow/task_tags.py) | Tag engines |
| [`message_store.py`](../src/duraflow/message_store.py) / [`message_postgres.py`](../src/duraflow/message_postgres.py) | Atomic journals |
| [`message_runtime.py`](../src/duraflow/message_runtime.py) | Consumer lifecycle and outbox relay |
| [`transport.py`](../src/duraflow/transport.py) / [`broker.py`](../src/duraflow/broker.py) | Native and memory messaging |

Protocol-1 polling components are retained explicitly for draining old histories.
They do not drive protocol-2 execution. See [cutover](6_operations.md#upgrading-from-protocol-1).
