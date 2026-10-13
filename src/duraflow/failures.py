"""Bounded group errors shared by collection and replay of older histories."""

from typing import Any

from .contracts import ProtocolError, TaskFailure, encode


def group_failure(failures: list[dict[str, Any]]) -> dict[str, Any]:
    """Keep a deterministic prefix that can be reconstructed as a TaskFailure.

    Individual node errors remain intact. Check the complete payload contract,
    including nesting depth: even one valid cause can exceed it once wrapped.
    """
    error = {"code": "GROUP_FAILURE", "message": "One or more group members failed", "causes": failures}
    try:
        return encode(error)
    except ProtocolError:
        pass
    lower, upper = 0, len(failures)
    result = {**error, "causes": [], "omitted_causes": len(failures)}
    while lower + 1 < upper:
        middle = (lower + upper) // 2
        candidate = {**error, "causes": failures[:middle], "omitted_causes": len(failures) - middle}
        try:
            candidate = encode(candidate)
        except ProtocolError:
            upper = middle
        else:
            lower, result = middle, candidate
    return result


def command_failure(record: dict[str, Any]) -> TaskFailure:
    """Allow pre-1.1.1 group summaries to reach workflow exception handlers."""
    error = record["error"]
    try:
        return TaskFailure(error)
    except ProtocolError:
        if (
            record["spec"]["kind"] not in {"gather", "broadcast"}
            or len(record["members"]) <= 1
            or error.get("code") != "GROUP_FAILURE"
            or not isinstance(error.get("causes"), list)
            or "omitted_causes" in error
        ):
            raise
        return TaskFailure(group_failure(error["causes"]))
