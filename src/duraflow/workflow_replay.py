"""A disposable coroutine driver with non-blocking durable registrations."""

from __future__ import annotations

from typing import Any, Callable, Generator, Generic, TypeVar

from .channels import Channel, ChannelRef
from .contracts import (
    NonDeterminism,
    ProtocolError,
    TaskFailure,
    UnsupportedWorkflow,
    decode,
    encode,
    fingerprint,
    schema_id,
    name,
    CODEC_VERSION,
    TaskRef,
    TaskOptions,
    SignalRef,
    WorkflowRef,
    WorkflowDefinition,
)
from .replay import Activation, Operation, WorkflowContext as ReplayContext


T = TypeVar("T")


class Future(Generic[T]):
    def __init__(self, context: WorkflowContext, handle: str, decoder: Callable[[Any], T]):
        self.context, self.id, self.decoder = context, handle, decoder

    def result(self) -> Operation[T]:
        return self.context._op({"kind": "future", "handle": self.id}, self.decoder)

    def __await__(self) -> Generator[Operation[T], Any, T]:
        return self.result().__await__()


class WorkflowContext(ReplayContext):
    def __init__(self) -> None:
        super().__init__()
        self.registrations: list[dict[str, Any]] = []
        self.registration_count = 0

    def register(self, spec: dict[str, Any]) -> str:
        self.check_active()
        handle = f"registration-{self.registration_count}"
        self.registration_count += 1
        self.registrations.append({**spec, "handle": handle, "detached": True})
        return handle

    def channel(self, ref: ChannelRef[T]) -> Channel[T]:
        return Channel(self, ref)

    def call(
        self, ref: TaskRef[Any, T], value: Any, *, options: TaskOptions | None = None, tags: tuple[str, ...] = ()
    ) -> Operation[T]:
        if len(tags) > 32:
            raise ValueError("Task tag limit exceeded")
        for tag in tags:
            name(tag)
        operation = super().call(ref, value, options=options)
        if tags:
            operation.spec["tags"] = sorted(set(tags))
        return operation

    def wait_signal(self, ref: SignalRef[T], *, timeout: float | None = None) -> Operation[T]:
        return self.channel(ChannelRef(ref.name, ref.payload_type)).receive(max_signals=1).next(timeout=timeout)

    def dispatch(
        self, ref: TaskRef[Any, T], value: Any, *, options: TaskOptions | None = None, tags: tuple[str, ...] = ()
    ) -> Future[T]:
        operation = self.call(ref, value, options=options, tags=tags)
        return Future(self, self.register(operation.spec), operation.decoder)

    def child(self, ref: WorkflowRef[Any, T], value: Any, *, abandon_on_parent_close: bool = False) -> Operation[T]:
        operation = super().child(ref, value, abandon_on_parent_close=abandon_on_parent_close)
        if ref.build_id:
            operation.spec["manifest"] = ref.manifest
        return operation

    def timer(self, seconds: float) -> Future[None]:
        operation = self.sleep(seconds)
        return Future(self, self.register(operation.spec), operation.decoder)

    def gather(self, *operations: Any) -> Operation[list[Any]]:
        return super().gather(*(op.result() if isinstance(op, Future) else op for op in operations))

    def race(self, *operations: Any) -> Operation[Any]:
        return super().race(*(op.result() if isinstance(op, Future) else op for op in operations))

    def send_signal(
        self, workflow: Any, workflow_id: str, channel: ChannelRef[Any], value: Any, *, signal_id: str
    ) -> Operation[None]:
        return self._op(
            {
                "kind": "send_signal",
                "workflow": workflow.name,
                "workflow_id": workflow_id,
                "channel": channel.name,
                "schema": schema_id(channel.payload_type),
                "payload": encode(value, channel.payload_type),
                "signal_id": signal_id,
            }
        )


def execute(definition: WorkflowDefinition, state: dict[str, Any], *, max_steps: int = 10000) -> Activation:
    """Return ordered new commands; neither user code nor callbacks touch storage."""
    if state.get("codec_version") != CODEC_VERSION or state.get("execution_protocol") != 2:
        raise ProtocolError("Unsupported workflow execution protocol or codec")
    if definition.manifest != state["manifest"]:
        raise NonDeterminism("Pinned implementation does not match")
    ctx = WorkflowContext()
    coroutine = definition.function(ctx, decode(state["input"], definition.ref.input_type))
    records, cursor = state["commands"], 0
    value, error = None, None
    try:
        for _ in range(max_steps):
            finished = False
            try:
                token = coroutine.throw(error) if error is not None else coroutine.send(value)
            except StopIteration as stop:
                finished, token, value = True, None, stop.value
            except TaskFailure as exc:
                if cursor != len(records):
                    raise NonDeterminism("Workflow failed before recorded history") from exc
                return Activation("failed", exc.error)
            specs = ctx.registrations
            ctx.registrations = []
            if not finished:
                if not isinstance(token, Operation) or token.context is not ctx:
                    raise UnsupportedWorkflow("Only durable operations may suspend a workflow")
                specs = [*specs, token.spec]
            new = []
            awaited = None
            for spec in specs:
                if cursor < len(records):
                    record = records[cursor]

                    def contract(value: dict[str, Any]) -> dict[str, Any]:
                        return {
                            key: [contract(m) for m in item] if key == "members" else item
                            for key, item in value.items()
                            if key != "manifest"
                        }

                    if fingerprint(contract(record["spec"])) != fingerprint(contract(spec)):
                        raise NonDeterminism(f"Command {cursor} differs from committed history")
                    if not spec.get("detached"):
                        awaited = record
                else:
                    new.append(spec)
                cursor += 1
            if new:
                return Activation("schedule", new)
            if finished:
                if cursor != len(records):
                    raise NonDeterminism("Workflow returned before recorded history")
                return Activation("completed", encode(value, definition.ref.output_type))
            assert awaited is not None
            if awaited["state"] == "pending":
                return Activation("waiting")
            error, value = None, None
            if awaited["state"] == "error":
                error = TaskFailure(awaited["error"])
            else:
                value = awaited["result"]
        raise UnsupportedWorkflow("Replay operation budget exceeded")
    except ProtocolError:
        raise
    except Exception as exc:
        if cursor < len(records):
            raise NonDeterminism("Workflow raised before recorded history") from exc
        return Activation("failed", {"code": "WORKFLOW_ERROR", "message": type(exc).__name__})
    finally:
        ctx.active = False
        try:
            coroutine.close()
        except (RuntimeError, UnsupportedWorkflow) as exc:
            raise UnsupportedWorkflow("Durable cleanup is unsupported") from exc
        if ctx.retirement_violation:
            raise UnsupportedWorkflow("Durable cleanup is unsupported")
