"""Recoverable replay-byte limits and bounded diagnostic responses."""

from typing import TYPE_CHECKING, Any

from .contracts import MessageSizeLimit

if TYPE_CHECKING:
    from .workflow_engine import Transition


def summary(state: dict[str, Any]) -> dict[str, Any]:
    omitted = {
        "input",
        "result",
        "commands",
        "nodes",
        "history",
        "channels",
        "signal_keys",
        "carried_signals",
        "actions",
    }
    return {
        **{key: value for key, value in state.items() if key not in omitted},
        "details_omitted": "SNAPSHOT_LIMIT",
        "command_count": len(state["commands"]),
        "node_count": len(state["nodes"]),
        "history_count": len(state["history"]),
    }


def preserve_results(transition: "Transition") -> None:
    """Commit valid observations even when their next activation cannot fit."""
    for publication in list(transition.outgoing):
        try:
            publication.message.to_bytes()
        except MessageSizeLimit:
            if publication.message.kind != "activate":
                raise
            transition.outgoing.remove(publication)
            transition.aggregate["active"] = None
            transition.state["status"] = "BLOCKED"
            transition.state["blocked_reason"] = "SNAPSHOT_LIMIT"
            transition.notify()
