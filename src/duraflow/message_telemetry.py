"""Small journal projections; metrics never scan workflow JSON histories."""

from typing import Any


def head_status(state: dict[str, Any]) -> tuple[str | None, str | None]:
    run = state.get("runs", {}).get(state.get("current"), {})
    return run.get("namespace"), run.get("status")


def sample(now: float, counts: dict[str, int], oldest: float | None) -> dict[str, Any]:
    return {
        "version": 2,
        "observed_at": now,
        "statuses": counts,
        "outbox_age": max(0, now - oldest) if oldest is not None else 0,
    }
