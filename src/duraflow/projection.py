"""Pure, conservative projection of a run's next outstanding obligation."""

from __future__ import annotations

from .contracts import TERMINAL
from .storage import State


def next_due(state: State, now: float, *, reconcile_interval: float = 10.0) -> float | None:
    if state.get("archived"):
        return None
    times: list[float] = []
    cancelling = state["status"] in {"CANCELLING", "CANCELLED", "TERMINATED"}
    for item in state["outbox"].values():
        if not item["delivered"]:
            if cancelling and item["metadata"]["kind"] != "wake":
                times.append(0.0)
            else:
                times.append(max(item.get("next_attempt_at", 0), item["lease_until"]))
    if state["status"] == "BLOCKED":
        return min(times) if times else None
    commands = state["commands"]
    if state["status"] not in TERMINAL:
        if not commands or commands[-1]["state"] != "pending":
            times.append(0.0)
        else:
            command = commands[-1]
            resolved = [state["nodes"][key]["state"] in {"done", "error"} for key in command["members"]]
            if all(resolved) or (command["spec"]["kind"] == "race" and any(resolved)):
                times.append(0.0)
    for node in state["nodes"].values():
        kind = node["spec"]["kind"]
        if (
            kind == "child"
            and state["status"] in TERMINAL | {"CANCELLING"}
            and not node["spec"].get("abandon")
            and not node.get("child_close_confirmed")
        ):
            # Absence is not proof that a concurrent child-start cannot commit.
            times.append(now + 1.0)
        if node["state"] != "pending":
            continue
        if kind == "call":
            current = node["attempts"][-1]
            options = node["spec"]["options"]
            if current["observation"] is not None:
                times.append(0.0)
                continue
            if cancelling or node.get("cancel_requested"):
                if current["started_at"] is None or current["deferred"] is not None:
                    times.append(0.0)
                else:
                    times.append(current["lease_until"])
            if options["overall_timeout"] is not None:
                times.append(node["created_at"] + options["overall_timeout"])
            if current["started_at"] is not None and options["attempt_timeout"] is not None:
                times.append(current["started_at"] + options["attempt_timeout"])
            if current["started_at"] is None and options["schedule_timeout"] is not None:
                times.append(current["not_before"] + options["schedule_timeout"])
            if current["deferred"] is not None:
                times.append(current["deferred"]["expires_at"])
            elif not cancelling and not node.get("cancel_requested"):
                reconcile = current["last_dispatch"] + reconcile_interval if current["dispatched"] else 0.0
                times.append(max(current["not_before"], current["lease_until"], reconcile))
        elif cancelling or node.get("cancel_requested"):
            times.append(0.0)
        elif kind == "sleep":
            times.append(node["due_at"])
        elif kind == "signal":
            if any(not signal["consumed"] and signal["name"] == node["spec"]["name"] for signal in state["signals"]):
                times.append(0.0)
            elif node["due_at"] is not None:
                times.append(node["due_at"])
        elif kind == "publish":
            if state["outbox"][node["outbox_id"]]["delivered"]:
                times.append(0.0)
        elif kind == "child":
            times.append(now + 1.0 if node.get("child_started") else 0.0)
        else:
            times.append(0.0)
    return float(min(times)) if times else None
