"""Pure outcome and join transitions shared by both execution protocols."""

from typing import Any

from .contracts import TERMINAL
from .lifecycle import accepted_before_cancel, deadline_error
from .state import attempt, event, finish_node
from .storage import State, clone


def error_data(code: str, message: str | None = None) -> dict[str, Any]:
    return {"code": code, "message": message or code}


def settle_task(state: State, node: State, now: float) -> None:
    current, options = node["attempts"][-1], node["spec"]["options"]
    observation = current["observation"]
    versioned = state.get("lifecycle_version", 1) >= 2
    decision_time = observation.get("recorded_at", now) if versioned and observation is not None else now
    cancel = node.get("cancel_requested") and not (versioned and accepted_before_cancel(node))
    orphan = (
        node.get("cancel_policy", 1) >= 2
        and current["started_at"] is not None
        and observation is None
        and (current["lease_until"] <= now or current["deferred"] is not None)
    )
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
    can_retry = (
        error["code"] in policy["retry_codes"]
        and error["code"] not in {"OVERALL_TIMEOUT", "CANCELLED", "TERMINATED"}
        and not node.get("cancel_requested")
    )
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


def collect(state: State, now: float) -> None:
    for record in state["commands"]:
        if record["state"] != "pending":
            continue
        nodes = [state["nodes"][node_id] for node_id in record["members"]]
        kind = record["spec"]["kind"]
        if kind == "race":
            candidates = [(i, node) for i, node in enumerate(nodes) if node["state"] in {"done", "error"}]
            if not candidates:
                continue
            i, winner = min(candidates, key=lambda pair: pair[1]["accepted_seq"])
            record["state"], record["error"] = winner["state"], winner["error"]
            record["result"] = {"index": i, "value": clone(winner["result"])}
        elif all(node["state"] in {"done", "error"} for node in nodes):
            failures = [node["error"] for node in nodes if node["state"] == "error"]
            if failures:
                record["state"] = "error"
                record["error"] = (
                    failures[0]
                    if len(nodes) == 1
                    else {"code": "GROUP_FAILURE", "message": "One or more group members failed", "causes": failures}
                )
            else:
                record["state"] = "done"
                values = [clone(node["result"]) for node in nodes]
                record["result"] = values if kind in {"gather", "broadcast"} else values[0]
        else:
            continue
        event(state, "command_resolved", now, command_id=record["id"], outcome=record["state"])
