# 0. Duraflow guide index

These guides describe the message-driven runtime in the current source checkout.
Files are numbered in reading order.

1. [Getting started](1_getting_started.md)
2. [Writing workflows](2_workflows.md)
3. [Signals and channels](3_signals.md)
4. [Broadcast and join](4_broadcast_and_join.md)
5. [Running distributed services](5_distributed.md)
6. [Operations](6_operations.md)
7. [Testing and contributing](7_testing.md)
8. [Internal architecture](8_architecture.md)

## Vocabulary

| Concept | Meaning |
| --- | --- |
| Workflow | Deterministic async orchestration code |
| Task | A sync or async business function executed by a task worker |
| Contract | A named, typed reference to a workflow, task, channel, or topic |
| Registry | Implementations available to a workflow or task worker |
| Logical workflow ID | Stable identity across run rollover |
| Run ID | Identity of one replay history |
| Activation | One disposable replay of a workflow snapshot |
| Signal stream | Registered reception with a durable buffer and repeated reads |
| Inbox | Consumed-message identities used to prevent duplicate transitions |
| Outbox | Outgoing messages committed atomically with state |
| Task journal | Service-owned execution records, leases, results, and outgoing events |

Existing protocol-1 executions use explicit legacy classes and `--legacy` CLI
commands. Their lifecycle remains separate; see [cutover](6_operations.md#upgrading-from-protocol-1).
Historical specifications and verification reports are retained in [`docs/`](../docs/).

[Back to the project](../README.md)
