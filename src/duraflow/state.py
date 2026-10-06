"""State constructors and pure transition helpers shared by runtime roles."""

from __future__ import annotations

from typing import Any, Callable
from uuid import NAMESPACE_URL, uuid5

from .contracts import Conflict, WorkflowDefinition, fingerprint
from .storage import State, Store, clone


def identity(run_id: str, key: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"duraflow/v1/{run_id}/{key}"))


def route(namespace: str, ref: dict[str, Any]) -> str:
    return f"persistent://public/default/df-{len(namespace)}-{namespace}-{ref['name']}-v{ref['version']}"


def subscription(namespace: str, handler: str) -> str:
    return f"df-{len(namespace)}-{namespace}-{handler}"


def event(state: State, kind: str, now: float, **details: Any) -> int:
    state["sequence"] += 1
    state["history"].append({"sequence": state["sequence"], "kind": kind, "time": now, **details})
    return int(state["sequence"])


def outbox(state: State, key: str, kind: str, topic: str, payload: Any, now: float, **metadata: Any) -> str:
    event_id = identity(state["run_id"], key)
    envelope = {
        "v": 1,
        "kind": kind,
        "namespace": state["namespace"],
        "run_id": state["run_id"],
        "event_id": event_id,
        **metadata,
    }
    existing = state["outbox"].get(event_id)
    item = {
        "topic": topic,
        "payload": clone(payload),
        "metadata": envelope,
        "delivered": False,
        "owner": None,
        "lease_until": 0,
        "attempts": 0,
        "created_at": now,
    }
    if existing is not None:
        if fingerprint([existing["topic"], existing["payload"], existing["metadata"]]) != fingerprint(
            [topic, payload, envelope]
        ):
            raise Conflict("Outbox identity collision")
    else:
        state["outbox"][event_id] = item
    return event_id


def wake(state: State, key: str, now: float) -> None:
    outbox(
        state,
        key,
        "wake",
        f"persistent://public/default/df-{len(state['namespace'])}-{state['namespace']}-events",
        {},
        now,
    )


def new_run(
    definition: WorkflowDefinition,
    namespace: str,
    run_id: str,
    workflow_id: str,
    value: Any,
    now: float,
    tags: tuple[str, ...] = (),
) -> State:
    state = {
        "namespace": namespace,
        "run_id": run_id,
        "workflow_id": workflow_id,
        "manifest": definition.manifest,
        "input": clone(value),
        "revision": 0,
        "status": "PENDING",
        "commands": [],
        "nodes": {},
        "history": [],
        "signals": [],
        "signal_keys": {},
        "outbox": {},
        "inbox": {},
        "actions": {},
        "sequence": 0,
        "created_at": now,
        "finished_at": None,
        "result": None,
        "error": None,
        "blocked_reason": None,
        "tags": sorted(set(tags)),
        "archived": False,
        "continued_run_id": None,
    }
    event(state, "started", now)
    wake(state, "start", now)
    return state


async def mutate(store: Store, namespace: str, run_id: str, change: Callable[[State], Any]) -> Any:
    for _ in range(64):
        state = await store.load(namespace, run_id)
        before = fingerprint(state)
        result = change(state)
        if fingerprint(state) == before or await store.save(state, state["revision"]):
            return result
    raise Conflict("Concurrent updates exceeded retry budget; retry the operation")


def attempt(number: int, now: float) -> dict[str, Any]:
    return {
        "number": number,
        "epoch": 0,
        "owner": None,
        "lease_until": 0,
        "created_at": now,
        "started_at": None,
        "not_before": now,
        "observation": None,
        "dispatched": False,
        "last_dispatch": 0,
        "deferred": None,
        "progress": None,
    }


def finish_node(
    state: State, node: State, now: float, *, result: Any = None, error: dict[str, Any] | None = None
) -> None:
    if node["state"] != "pending":
        return
    node["state"] = "error" if error is not None else "done"
    node["error"], node["result"] = error, clone(result)
    node["accepted_seq"] = event(
        state,
        "operation_resolved",
        now,
        node_id=node["id"],
        outcome=node["state"],
        code=error.get("code") if error else None,
    )
