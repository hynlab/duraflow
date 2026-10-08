# Message-driven runtime (protocol 2)

The runtime follows the component boundaries documented by Infinitic: a broker
client, workflow state engine, workflow executor, service executor, and tag engine.
Implementation and JSON wire contracts are independently authored Python.

## Ownership and execution

Client -> workflow inbox -> state engine -> workflow execution -> workflow worker
-> workflow inbox -> task execution -> task worker -> workflow inbox -> ...

Only the workflow engine modifies workflow state. Executors receive immutable
requests and return messages. A state transaction commits the consumed message
identity, state, and outgoing messages together; ACK follows commit. Outgoing
messages have stable identities and are independently retried by an outbox relay.
No user function or broker call runs inside a state transaction.

Workflow messages use a logical-instance key and Key_Shared subscriptions. Key
affinity is an optimization, not the concurrency boundary: transactional state
locking and activation IDs fence stale executors. Events arriving during replay
are persisted until the activation's commands have committed. Causal predecessors
are explicit; there is no fabricated total ordering across independent producers.

Timers, deadlines and application retries are delayed broker messages. They carry
generation IDs; canceled or superseded reservations cannot change current state.
The outbox may poll its indexed delivery obligations. Normal workflow advancement
never scans workflow records or polls results.

## Channels

`ctx.channel(ref).receive()` registers a durable stream without blocking. It
returns a stream whose `next()` waits for the next accepted signal. Registration
is committed before tasks dispatched in the same replay segment are published.
Signals before registration are discarded. After registration, matching signals
are buffered, deduplicated and consumed in the engine's recorded acceptance order.
`max_signals`, typed contracts and declarative attribute filters bound reception.
Signal registration, buffered payloads and consumption indexes survive restarts.

## Cutover

Protocol 1 runs retain their existing lifecycle and pre-wait signal semantics.
Drain them using the explicit legacy CLI/runtime. Protocol 2 uses separate topics
and state tables; a protocol-1 worker cannot consume a protocol-2 command. No
automatic conversion of running histories is performed. Start new executions
using the message-driven client and workers after provisioning subscriptions.

## References

- https://docs.infinitic.io/docs/components/infrastructure
- https://docs.infinitic.io/docs/workflows/deployment
- https://docs.infinitic.io/docs/workflows/signals
- https://docs.infinitic.io/docs/clients/start-workflow
- https://infinitic.substack.com/p/infinitic-0180-how-we-fixed-a-critical
