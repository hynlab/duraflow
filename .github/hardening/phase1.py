"""Phase 1: precise lifecycle transitions and isolated, version-routed replay."""
import asyncio
import json
import sys
from pathlib import Path
from helpers import add_method, function, replace, write

sys.path.insert(0, str(Path.cwd()))
# Freeze reference data using the unchanged alpha runtime BEFORE changing files.
from duraflow import Registry
from duraflow.contracts import encode, schema_id
from duraflow.testing import TestEnvironment
from tests.codec_payloads import Receipt, sample
from tests.test_engine import sequence, double

async def freeze_alpha():
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 3, request_id="frozen-alpha")
        assert await env.run(handle) == 13
        write('tests/fixtures/alpha_sequence.json', json.dumps(await handle.describe(), indent=2) + '\n')

asyncio.run(freeze_alpha())
write('tests/fixtures/codec_v1.json', json.dumps({'schema': schema_id(Receipt), 'payload': encode(sample(), Receipt)}, indent=2) + '\n')

replace('src/duraflow/contracts.py', 'REPLAY_VERSION = 1', 'REPLAY_VERSION = 1\nCODEC_VERSION = 1')
add_method('src/duraflow/contracts.py', 'Registry', '''
def match_manifest(self, manifest: dict[str, Any]) -> WorkflowDefinition | None:
    definition = self.workflows.get(f"{manifest.get('name')}:v{manifest.get('version')}")
    if definition is None or definition.manifest != manifest:
        return None
    return definition
''')
replace('src/duraflow/state.py', '"manifest": definition.manifest,', '"manifest": definition.manifest,\n        "lifecycle_version": 2,\n        "codec_version": 1,')
replace('src/duraflow/replay.py', 'from .contracts import (', 'from .contracts import (\n    CODEC_VERSION,')
replace('src/duraflow/replay.py', '    if state["manifest"] != definition.manifest:', '    if state.get("codec_version", 1) != CODEC_VERSION:\n        raise ProtocolError("Unsupported durable codec version")\n    if state["manifest"] != definition.manifest:')
replace('src/duraflow/coordinator.py', 'from .replay import replay', 'from .executor import InlineReplayExecutor, ReplayExecutor\nfrom .lifecycle import accepted_before_cancel, deadline_error')
function('src/duraflow/coordinator.py', 'Engine.__init__', '''
def __init__(self, store: Store, transport: Transport, registry: Registry, *,
             namespace: str = "default", clock: Clock | None = None,
             batch_size: int = 100, max_commands: int = 1000, reconcile_interval: float = 10.0,
             replay_executor: ReplayExecutor | None = None):
    duration(reconcile_interval)
    if not 1 <= batch_size <= 1000 or not 1 <= max_commands <= 10000:
        raise ValueError("Invalid batch/history limits")
    self.store, self.transport, self.registry = store, transport, registry
    self.namespace, self.clock = name(namespace), clock or Clock()
    self.batch_size, self.max_commands = batch_size, max_commands
    self.reconcile_interval, self.cursor = reconcile_interval, ""
    self.replay_executor = replay_executor or InlineReplayExecutor()
    self.client = Client(store, registry, namespace=namespace, clock=self.clock)
    self.metrics = {"activations": 0, "cas_conflicts": 0, "publications": 0,
                    "transport_errors": 0, "unsupported_activations": 0}
''')
replace('src/duraflow/coordinator.py', '        original, now = fingerprint(state), self.clock.now()', '''        if (state["status"] not in TERMINAL | {"CANCELLING"}
                and self.registry.match_manifest(state["manifest"]) is None):
            self.metrics["unsupported_activations"] += 1
            return
        original, now = fingerprint(state), self.clock.now()''')
replace('src/duraflow/coordinator.py', 'activation = replay(definition, state)', 'activation = await self.replay_executor.execute(definition, state)')
add_method('src/duraflow/coordinator.py', 'Engine', '''
async def close(self) -> None:
    await self.replay_executor.close()
''')
function('src/duraflow/coordinator.py', 'settle_task', '''
def settle_task(state: State, node: State, now: float) -> None:
    """Legacy ordering is retained; new runs decide deadlines at result acceptance."""
    current, options = node["attempts"][-1], node["spec"]["options"]
    observation = current["observation"]
    versioned = state.get("lifecycle_version", 1) >= 2
    decision_time = observation.get("recorded_at", now) if versioned and observation is not None else now
    cancel = node.get("cancel_requested") and not (versioned and accepted_before_cancel(node))
    orphan = (node.get("cancel_policy", 1) >= 2 and current["started_at"] is not None
              and observation is None and (current["lease_until"] <= now or current["deferred"] is not None))
    error = None
    if state["status"] == "TERMINATED":
        error = error_data("TERMINATED")
    elif cancel and (current["started_at"] is None or observation is not None or orphan):
        error = error_data("CANCELLED")
        if orphan:
            node["external_outcome"] = "unknown"
            current["epoch"] += 1
            current["owner"], current["lease_until"] = None, 0
            event(state, "cancellation_owner_lost", now, node_id=node["id"], external_outcome="unknown")
    else:
        error = deadline_error(node, decision_time)
        if error is None:
            if observation is None:
                return
            error = observation["error"]
    if error is None:
        finish_node(state, node, now, result=observation["result"])
        return
    policy = options["retry"]
    can_retry = (error["code"] in policy["retry_codes"]
                 and error["code"] not in {"OVERALL_TIMEOUT", "CANCELLED", "TERMINATED"}
                 and not node.get("cancel_requested"))
    current["final_error"] = clone(error)
    if can_retry and current["number"] < policy["max_attempts"]:
        next_attempt = attempt(current["number"] + 1, now)
        delay = min(policy["max_delay"], policy["delay"] * policy["multiplier"] ** min(current["number"] - 1, 30))
        next_attempt["not_before"] = now + delay
        node["attempts"].append(next_attempt)
        event(state, "retry_scheduled", now, node_id=node["id"], attempt=next_attempt["number"])
    elif can_retry and policy["exhausted"] == "block" and state["status"] not in TERMINAL:
        node["state"], state["status"] = "blocked", "BLOCKED"
        state["blocked_reason"] = f"Retry exhausted for {node['id']}"
        event(state, "task_blocked", now, node_id=node["id"])
    else:
        finish_node(state, node, now, error=error)
''')
replace('src/duraflow/runner.py', 'from .transport import Delivery, Transport', 'from .transport import Delivery, Transport\nfrom .lifecycle import record_observation')
function('src/duraflow/runner.py', 'Worker._observe', '''
async def _observe(self, context: TaskContext, result: Any, error: dict[str, Any] | None) -> None:
    observation = {"result": clone(result), "error": clone(error)}
    def change(state: State) -> None:
        current = context._current(state)
        now = self.clock.now()
        if current["lease_until"] <= now and current["deferred"] is None:
            raise Conflict("Cannot record an outcome after lease expiry")
        if record_observation(state, state["nodes"][context.node_id], observation, now, kind="task_observed"):
            wake(state, f"observation/{context.node_id}/{context.attempt}/{context.lease_epoch}", now)
    try:
        await mutate(self.store, self.namespace, context.run_id, change)
    except Conflict:
        self.metrics["stale_results"] += 1
        def stale(state: State) -> None:
            if state["archived"]:
                return
            node = state["nodes"].get(context.node_id)
            if node is None:
                return
            key = f"{context.attempt}/{context.lease_epoch}"
            observations = node.setdefault("stale_observations", {})
            if len(observations) < 100 and key not in observations:
                observations[key] = fingerprint(observation)
                event(state, "stale_observation", self.clock.now(), node_id=context.node_id,
                      attempt=context.attempt, epoch=context.lease_epoch)
        await mutate(self.store, self.namespace, context.run_id, stale)
''')
replace('src/duraflow/client.py', 'from .storage import State, Store', 'from .storage import State, Store\nfrom .lifecycle import record_observation')
function('src/duraflow/client.py', 'Client.complete_external', '''
async def complete_external(self, token: str, value: Any = None, *, ref: TaskRef[Any, Any],
                            error: dict[str, Any] | None = None) -> bool:
    try:
        run_id, node_id, number, epoch, _ = token.split("/", 4)
        number_int, epoch_int = int(number), int(epoch)
    except (ValueError, AttributeError):
        raise ValueError("Invalid completion token") from None
    digest = hashlib.sha256(token.encode()).hexdigest()
    observation = {"result": encode(value, ref.output_type) if error is None else None,
                   "error": TaskFailure(error).error if error is not None else None}
    def change(state: State) -> bool:
        node = state["nodes"].get(node_id)
        if node is None or node["spec"]["kind"] != "call" or node["spec"]["ref"] != ref.descriptor():
            raise Conflict("Unknown or incompatible delegated invocation")
        current = node["attempts"][-1]
        deferred = current.get("deferred")
        if (current["number"] != number_int or current["epoch"] != epoch_int or not deferred
                or not hmac.compare_digest(deferred["token_hash"], digest)):
            raise Conflict("Completion token is invalid or superseded")
        now = self.clock.now()
        if current["observation"] is not None:
            return record_observation(state, node, observation, now, kind="external_observation")
        if (node["state"] != "pending" or state["status"] in (TERMINAL - {"COMPLETED", "FAILED"})
                or state["status"] == "CANCELLING" or deferred["expires_at"] <= now):
            raise Conflict("Delegated invocation no longer accepts completion")
        changed = record_observation(state, node, observation, now, kind="external_observation")
        if changed:
            wake(state, f"external/{node_id}/{number_int}/{epoch_int}", now)
        return changed
    return bool(await mutate(self.store, self.namespace, run_id, change))
''')
replace('src/duraflow/client.py', '                        node["cancel_requested"] = True', '                        node["cancel_requested"] = True\n                        node["cancel_policy"] = 2\n                        node.setdefault("cancel_requested_seq", state["sequence"] + 1)')
replace('src/duraflow/cli.py', 'from .coordinator import Engine', 'from .coordinator import Engine\nfrom .executor import ProcessReplayExecutor')
# Insert after runtime construction without depending on its formatter layout.
replace('src/duraflow/cli.py', '    stop = asyncio.Event()', '    if isinstance(runtime, Engine):\n        runtime.replay_executor = ProcessReplayExecutor(args.app)\n    stop = asyncio.Event()')
replace('src/duraflow/cli.py', '        if isinstance(runtime, Worker):\n            await runtime.close()', '        await runtime.close()')
# Preserve the changed-command assertion; only the missing-implementation behavior changes.
replace('tests/test_engine.py', '        env.engine.registry = Registry()\n        await env.engine.tick()\n        assert (await h.describe())["status"] == "BLOCKED"', '        env.engine.registry = Registry()\n        await env.engine.tick()\n        assert (await h.describe())["status"] == "WAITING"\n        assert env.engine.metrics["unsupported_activations"] == 1')
write('docs/hardening/phase1.md', '''
# Phase 1: execution correctness and isolation

A01 retains the native and unit acceptance baseline; no test gate is removed.
A02 adds ProcessReplayExecutor with a bounded reusable subprocess pool, JSON-only
IPC and separate bootstrap/activation deadlines. CLI engines select it. Inline
replay remains explicitly for tests/embedded trusted development. This is fault
containment, not a hostile-code security sandbox. A workflow must be registered
by an importable trusted app. Stuck code, including suspended cleanup, is killed
without blocking the coordinator event loop.

A03: explicit cancellation records policy 2. A lost worker's expired lease is
fenced before cancel termination; its external outcome is UNKNOWN, not rolled back.
A04: newly started runs pin lifecycle_version=2. Results carry a coordinator-store
mutation sequence and acceptance time. Polling delay does not turn a timely result
into a timeout. Exact deadline is expired. Old records without the version or time
retain legacy conservative ordering, never an invented acceptance timestamp.
The production authoritative-time transaction is the separate B02 prerequisite.

A05: only an exact manifest match may replay a run. A mismatching engine increments
an unsupported counter and leaves the durable execution untouched. True divergence
in a compatible implementation still blocks. Different registries/builds may
coexist; no latest-version substitution is permitted.

A06: durable codec_version=1 is explicit on new state, legacy absent means 1.
Frozen alpha sequence history and typed datetime/Decimal/dataclass payload/schema
fixtures are retained rather than regenerated by ordinary tests. Unknown codec
versions fail closed. Broader dependency/rolling-upgrade qualification is separate.
''')
