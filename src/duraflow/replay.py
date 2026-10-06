"""Disposable coroutine activations. No I/O and no suspended stacks retained."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Callable, Generator, Generic, TypeVar, cast
from uuid import UUID

from .contracts import (
    CODEC_VERSION,
    HandlerRef,
    NonDeterminism,
    ProtocolError,
    SignalRef,
    TaskFailure,
    TaskOptions,
    TaskRef,
    TopicRef,
    UnsupportedWorkflow,
    WorkflowDefinition,
    WorkflowRef,
    decode,
    duration,
    encode,
    fingerprint,
    schema_id,
)

T = TypeVar("T")


@dataclass(frozen=True)
class RaceResult:
    index: int
    value: Any


class BroadcastResult:
    def __init__(self, handlers: tuple[HandlerRef[Any, Any], ...], values: list[Any]):
        self._values = {h.subscription: decode(v, h.task.output_type) for h, v in zip(handlers, values, strict=True)}

    def __getitem__(self, handler: HandlerRef[Any, T]) -> T:
        return cast(T, self._values[handler.subscription])


class Operation(Generic[T]):
    """A cold command descriptor, not an asyncio task or a running RPC."""

    def __init__(
        self, context: WorkflowContext, spec: dict[str, Any], decoder: Callable[[Any], T] = lambda value: value
    ):
        self.context, self.spec, self.decoder = context, spec, decoder
        self.used = False

    def __await__(self) -> Generator[Operation[T], Any, T]:
        self.context.check_active()
        if self.used:
            raise UnsupportedWorkflow("A cold operation may only be consumed once")
        self.used = True
        value = yield self
        return self.decoder(value)


class WorkflowContext:
    def __init__(self) -> None:
        self.active = True
        self.retirement_violation = False

    def check_active(self) -> None:
        if not self.active:
            self.retirement_violation = True
            raise UnsupportedWorkflow("Durable operations in retirement/finalizers are unsupported")

    def _op(self, spec: dict[str, Any], decoder: Callable[[Any], Any] = lambda v: v) -> Operation[Any]:
        self.check_active()
        return Operation(self, spec, decoder)

    def call(self, ref: TaskRef[Any, T], value: Any, *, options: TaskOptions | None = None) -> Operation[T]:
        return self._op(
            {
                "kind": "call",
                "ref": ref.descriptor(),
                "input": encode(value, ref.input_type),
                "options": asdict(options or TaskOptions()),
            },
            lambda result: decode(result, ref.output_type),
        )

    def _group(self, kind: str, operations: tuple[Operation[Any], ...]) -> Operation[Any]:
        if not operations or len({id(op) for op in operations}) != len(operations):
            raise ValueError("A group requires distinct operations")
        for op in operations:
            if not isinstance(op, Operation) or op.context is not self or op.used:
                raise UnsupportedWorkflow("Groups require unused operations from this context")
            if op.spec["kind"] in {"gather", "race", "broadcast", "continue"}:
                raise UnsupportedWorkflow("Nested groups/rollover are unsupported; use child workflows")
        for op in operations:
            op.used = True

        def decode_group(result: Any) -> Any:
            if kind == "race":
                index = result["index"]
                return RaceResult(index, operations[index].decoder(result["value"]))
            return [op.decoder(v) for op, v in zip(operations, result, strict=True)]

        return self._op({"kind": kind, "members": [op.spec for op in operations]}, decode_group)

    def gather(self, *operations: Operation[Any]) -> Operation[list[Any]]:
        return self._group("gather", operations)

    def race(self, *operations: Operation[Any]) -> Operation[RaceResult]:
        return self._group("race", operations)

    def broadcast(
        self,
        topic: TopicRef[Any],
        value: Any,
        *,
        handlers: tuple[HandlerRef[Any, Any], ...],
        options: TaskOptions | None = None,
    ) -> Operation[BroadcastResult]:
        if not handlers or len({h.subscription for h in handlers}) != len(handlers):
            raise ValueError("Broadcast requires distinct named subscriptions")
        payload = encode(value, topic.payload_type)
        members = []
        for handler in handlers:
            if schema_id(topic.payload_type) != schema_id(handler.task.input_type):
                raise ValueError("Broadcast handler input contract differs from topic contract")
            members.append(
                {
                    "kind": "call",
                    "ref": handler.task.descriptor(),
                    "input": payload,
                    "options": asdict(options or TaskOptions()),
                    "handler": handler.subscription,
                }
            )
        return self._op(
            {
                "kind": "broadcast",
                "topic": topic.name,
                "payload_schema": schema_id(topic.payload_type),
                "input": payload,
                "members": members,
            },
            lambda results: BroadcastResult(handlers, results),
        )

    def publish(self, topic: TopicRef[Any], value: Any) -> Operation[str]:
        return self._op(
            {
                "kind": "publish",
                "topic": topic.name,
                "payload_schema": schema_id(topic.payload_type),
                "input": encode(value, topic.payload_type),
            }
        )

    def sleep(self, seconds: float) -> Operation[None]:
        duration(seconds)
        return self._op({"kind": "sleep", "seconds": seconds})

    def wait_signal(self, ref: SignalRef[T], *, timeout: float | None = None) -> Operation[T]:
        duration(timeout)
        return self._op(
            {"kind": "signal", "name": ref.name, "schema": schema_id(ref.payload_type), "timeout": timeout},
            lambda value: decode(value, ref.payload_type),
        )

    def now(self) -> Operation[datetime]:
        return self._op({"kind": "now"}, datetime.fromisoformat)

    def uuid(self) -> Operation[UUID]:
        return self._op({"kind": "uuid"}, UUID)

    def child(self, ref: WorkflowRef[Any, T], value: Any, *, abandon_on_parent_close: bool = False) -> Operation[T]:
        return self._op(
            {
                "kind": "child",
                "ref": ref.descriptor(),
                "input": encode(value, ref.input_type),
                "abandon": abandon_on_parent_close,
            },
            lambda result: decode(result, ref.output_type),
        )

    def continue_as_new(self, value: Any) -> Operation[None]:
        return self._op({"kind": "continue", "input": encode(value)})


@dataclass(frozen=True)
class Activation:
    kind: str
    value: Any = None


def replay(definition: WorkflowDefinition, state: dict[str, Any], *, max_steps: int = 10_000) -> Activation:
    if state.get("codec_version", 1) != CODEC_VERSION:
        raise ProtocolError("Unsupported durable codec version")
    if state["manifest"] != definition.manifest:
        raise NonDeterminism("Pinned implementation/codec/runtime identity does not match")
    ctx = WorkflowContext()
    coroutine = definition.function(ctx, decode(state["input"], definition.ref.input_type))
    cursor = 0
    value: Any = None
    error: TaskFailure | None = None
    try:
        while cursor <= max_steps:
            try:
                token = coroutine.throw(error) if error is not None else coroutine.send(value)
                error, value = None, None
            except StopIteration as stop:
                if cursor != len(state["commands"]):
                    raise NonDeterminism(f"Workflow returned before recorded command {cursor}")
                return Activation("completed", encode(stop.value, definition.ref.output_type))
            except TaskFailure as exc:
                if cursor != len(state["commands"]):
                    raise NonDeterminism(f"Workflow failed before recorded command {cursor}") from exc
                return Activation("failed", exc.error)
            if not isinstance(token, Operation) or token.context is not ctx:
                raise UnsupportedWorkflow("Only engine operations may suspend a workflow")
            if cursor >= len(state["commands"]):
                return Activation("schedule", token.spec)
            record = state["commands"][cursor]
            if fingerprint(record["spec"]) != fingerprint(token.spec):
                raise NonDeterminism(f"Command {cursor} differs from its committed history")
            cursor += 1
            if record["state"] == "pending":
                return Activation("waiting")
            if record["state"] == "error":
                error = TaskFailure(record["error"])
            else:
                value = decode(record["result"])
        raise UnsupportedWorkflow("Replay operation budget exceeded")
    except ProtocolError:
        raise
    except Exception as exc:
        if cursor != len(state["commands"]):
            raise NonDeterminism(f"Workflow raised before recorded command {cursor}") from exc
        return Activation("failed", {"code": "WORKFLOW_ERROR", "message": type(exc).__name__})
    finally:
        ctx.active = False
        try:
            coroutine.close()
        except (RuntimeError, UnsupportedWorkflow) as exc:
            raise UnsupportedWorkflow("Durable operations in suspended cleanup are unsupported") from exc
        if ctx.retirement_violation:
            raise UnsupportedWorkflow("Retirement attempted to schedule a durable operation")
