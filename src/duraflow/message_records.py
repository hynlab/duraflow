"""Auxiliary records participating in the consuming state's transaction.

Reads are point lookups; only touched records are written. No broker I/O occurs
inside this interface. Expiration is opt-in and must have an admission fence.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .messaging import Publication
from .storage import clone


class Records:
    def __init__(self, load: Callable[[str], dict[str, Any] | None]):
        self.load = load
        self.writes: dict[str, tuple[dict[str, Any], float | None]] = {}

    def get(self, key: str) -> dict[str, Any] | None:
        if key in self.writes:
            return clone(self.writes[key][0])
        value = self.load(key)
        return clone(value) if value is not None else None

    def put(self, key: str, value: dict[str, Any], *, expires_at: float | None = None) -> None:
        self.writes[key] = clone(value), expires_at


@dataclass
class RecordUpdate:
    callback: Callable[[dict[str, Any], float, Records], list[Publication]]


@dataclass
class LocalUpdate:
    """Update fenced by state instead of inbox: local attempts or idempotent retirement."""

    callback: Callable[[dict[str, Any], float], list[Publication]]
    create: bool = True


Update = Callable[[dict[str, Any], float], list[Publication]] | RecordUpdate | LocalUpdate


def apply_update(update: Update, state: dict[str, Any], now: float, records: Records) -> list[Publication]:
    if isinstance(update, RecordUpdate):
        return update.callback(state, now, records)
    if isinstance(update, LocalUpdate):
        return update.callback(state, now)
    return update(state, now)
