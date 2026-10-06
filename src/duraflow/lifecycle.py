"""Versioned outcome acceptance and deadline ordering for durable task attempts."""
from __future__ import annotations

from typing import Any

from .contracts import Conflict, fingerprint
from .state import event
from .storage import State, clone


def deadline_error(node: State, when: float) -> dict[str, Any] | None:
    current = node["attempts"][-1]
    options = node["spec"]["options"]
    checks = [
        ("OVERALL_TIMEOUT", options["overall_timeout"], node["created_at"]),
        ("ATTEMPT_TIMEOUT", options["attempt_timeout"], current["started_at"]),
        ("SCHEDULE_TIMEOUT", options["schedule_timeout"] if current["started_at"] is None else None,
         current["not_before"]),
    ]
    for code, duration, start in checks:
        if duration is not None and start is not None and when >= start + duration:
            return {"code": code, "message": code}
    delegated = current.get("deferred")
    if delegated is not None and when >= delegated["expires_at"]:
        return {"code": "DELEGATION_TIMEOUT", "message": "DELEGATION_TIMEOUT"}
    return None


def record_observation(state: State, node: State, observation: State, now: float, *, kind: str) -> bool:
    """Caller must commit this mutation with its fenced ownership/CAS transaction.

    The caller supplies the transaction's trusted clock, never a payload timestamp.
    Repeated acknowledgements compare business outcome only, not acceptance time.
    """
    current = node["attempts"][-1]
    previous = current["observation"]
    if previous is not None:
        previous_value = {key: previous[key] for key in ("result", "error")}
        if fingerprint(previous_value) != fingerprint(observation):
            raise Conflict("Conflicting repeated observation")
        return False
    sequence = event(state, kind, now, node_id=node["id"],
                     attempt=current["number"], epoch=current["epoch"])
    current["observation"] = clone(observation)
    if state.get("lifecycle_version", 1) >= 2:
        current["observation"].update(recorded_at=now, sequence=sequence)
    return True


def accepted_before_cancel(node: State) -> bool:
    observation = node["attempts"][-1]["observation"]
    return bool(observation is not None and "sequence" in observation
                and observation["sequence"] < node.get("cancel_requested_seq", -1))
