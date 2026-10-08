"""Typed, replay-safe signal streams and declarative payload filters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from .contracts import duration, encode, name, schema_id, canonical
from .replay import Operation, WorkflowContext

T = TypeVar("T")


@dataclass(frozen=True)
class ChannelRef(Generic[T]):
    name: str
    payload_type: Any = Any

    def __post_init__(self) -> None:
        name(self.name)


@dataclass(frozen=True)
class SignalFilter:
    """Declarative equality predicates on dotted JSON paths; never serialized code."""

    equals: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        encode(self.equals)
        if len(self.equals) > 32 or any(not path or len(path) > 256 for path in self.equals):
            raise ValueError("Invalid signal filter")

    def matches(self, payload: Any) -> bool:
        for path, expected in self.equals.items():
            current = payload
            for part in path.split("."):
                if not isinstance(current, dict) or part not in current:
                    return False
                current = current[part]
            if canonical(current) != canonical(expected):
                return False
        return True


class SignalStream(Generic[T]):
    def __init__(self, context: WorkflowContext, stream_id: str, payload_type: Any):
        self.context, self.id, self.payload_type = context, stream_id, payload_type
        self.index = 0

    def next(self, *, timeout: float | None = None) -> Operation[T]:
        from .contracts import decode

        duration(timeout)
        index = self.index
        self.index += 1
        return self.context._op(
            {"kind": "channel_next", "stream_id": self.id, "index": index, "timeout": timeout},
            lambda value: decode(value, self.payload_type),
        )


class Channel(Generic[T]):
    def __init__(self, context: Any, ref: ChannelRef[T]):
        self.context, self.ref = context, ref

    def receive(
        self, *, max_signals: int | None = None, filter: SignalFilter | None = None, payload_type: Any = None
    ) -> SignalStream[T]:
        if max_signals is not None and (type(max_signals) is not int or not 1 <= max_signals <= 10000):
            raise ValueError("max_signals must be in 1..10000")
        typ = self.ref.payload_type if payload_type is None else payload_type
        stream_id = self.context.register(
            {
                "kind": "channel_open",
                "channel": self.ref.name,
                "schema": schema_id(self.ref.payload_type),
                "accepted_schema": schema_id(typ),
                "filter": (filter or SignalFilter()).equals,
                "max_signals": max_signals,
            }
        )
        return SignalStream(self.context, stream_id, typ)
