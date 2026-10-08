"""Public typed contracts and strict JSON boundaries; no infrastructure imports."""

from __future__ import annotations

from contextvars import ContextVar

import hashlib
import inspect
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from functools import lru_cache
from types import SimpleNamespace
from typing import Any, Callable, Generic, TypeVar, get_type_hints

from pydantic import TypeAdapter

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT")
F = TypeVar("F", bound=Callable[..., Any])
MAX_PAYLOAD_BYTES = 262_144
PROTOCOL_VERSION = 1
REPLAY_VERSION = 1
CODEC_VERSION = 1
_STORE_TIME: ContextVar[float | None] = ContextVar("duraflow_store_time", default=None)
TERMINAL = frozenset({"COMPLETED", "FAILED", "CANCELLED", "TERMINATED", "CONTINUED"})


class DuraflowError(Exception):
    """Base class for public SDK errors."""


class Conflict(DuraflowError):
    pass


class NotFound(DuraflowError):
    pass


class ProtocolError(DuraflowError):
    pass


class NonDeterminism(ProtocolError):
    pass


class UnsupportedWorkflow(ProtocolError):
    pass


class WorkflowBlocked(DuraflowError):
    pass


class WorkflowFailed(DuraflowError):
    pass


class Archived(DuraflowError):
    pass


class TaskCancelled(DuraflowError):
    pass


class TaskFailure(DuraflowError):
    def __init__(self, error: dict[str, Any]):
        code = error.get("code", "TASK_FAILURE")
        message = error.get("message", code)
        if not isinstance(code, str) or not isinstance(message, str) or not code:
            raise ProtocolError("Remote errors require string code/message fields")
        self.error = encode({**error, "code": code[:128], "message": message[:1000]})
        self.code = code
        super().__init__(message)


def canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ProtocolError("Expected finite JSON data") from exc


def fingerprint(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def parse_json(raw: str | bytes) -> Any:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ProtocolError("Duplicate JSON object key")
            result[key] = value
        return result

    try:
        value = json.loads(raw, object_pairs_hook=pairs)
        canonical(value)
        return value
    except (ValueError, TypeError, RecursionError) as exc:
        raise ProtocolError("Malformed JSON") from exc


@lru_cache(maxsize=256)
def adapter(typ: Any) -> TypeAdapter[Any]:
    return TypeAdapter(typ)


def _finite(value: Any, depth: int = 0) -> None:
    if depth > 64:
        raise ProtocolError("Payload nesting exceeds 64 levels")
    if isinstance(value, float) and not math.isfinite(value):
        raise ProtocolError("Nonfinite numbers are unsupported")
    if isinstance(value, Decimal) and not value.is_finite():
        raise ProtocolError("Nonfinite decimals are unsupported")
    if isinstance(value, datetime) and value.utcoffset() != timedelta(0):
        raise ProtocolError("Datetimes must be timezone-aware UTC values")
    if isinstance(value, (set, frozenset)):
        raise ProtocolError("Unordered sets are not durable payloads; use an ordered list")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ProtocolError("Payload mappings require string keys")
            _finite(item, depth + 1)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _finite(item, depth + 1)


def encode(value: Any, typ: Any = Any) -> Any:
    try:
        checked = adapter(typ).validate_python(value, strict=True)
        _finite(adapter(typ).dump_python(checked, mode="python", warnings="error"))
        data = parse_json(adapter(typ).dump_json(checked, warnings="error"))
        if len(canonical(data).encode()) > MAX_PAYLOAD_BYTES:
            raise ProtocolError("Inline payload exceeds 256 KiB")
        return data
    except ProtocolError:
        raise
    except Exception as exc:
        raise ProtocolError("Payload does not satisfy its declared contract") from exc


def decode(value: Any, typ: Any = Any) -> Any:
    try:
        return adapter(typ).validate_json(canonical(value), strict=True)
    except Exception as exc:
        raise ProtocolError("Stored payload does not satisfy its declared contract") from exc


@lru_cache(maxsize=256)
def schema_id(typ: Any) -> str:
    return fingerprint(adapter(typ).json_schema())


def name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ValueError("Names must be 1..128 ASCII letters, digits, dots, dashes or underscores")
    return value


def duration(value: float | None) -> None:
    if value is not None and (isinstance(value, bool) or not math.isfinite(value) or value <= 0):
        raise ValueError("Durations must be finite and positive")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 1
    delay: float = 1.0
    multiplier: float = 2.0
    max_delay: float = 60.0
    retry_codes: tuple[str, ...] = ("TASK_ERROR", "ATTEMPT_TIMEOUT")
    exhausted: str = "raise"

    def __post_init__(self) -> None:
        if type(self.max_attempts) is not int or not 1 <= self.max_attempts <= 1000:
            raise ValueError("max_attempts must be in 1..1000")
        if self.exhausted not in {"raise", "block"}:
            raise ValueError("exhausted must be 'raise' or 'block'")
        for value in (self.delay, self.multiplier, self.max_delay):
            duration(value)
        if self.multiplier < 1 or any(not isinstance(code, str) for code in self.retry_codes):
            raise ValueError("Invalid retry policy")


@dataclass(frozen=True)
class TaskOptions:
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    attempt_timeout: float | None = None
    overall_timeout: float | None = None
    schedule_timeout: float | None = None

    def __post_init__(self) -> None:
        for value in (self.attempt_timeout, self.overall_timeout, self.schedule_timeout):
            duration(value)


@dataclass(frozen=True)
class TaskRef(Generic[InputT, OutputT]):
    name: str
    input_type: Any = Any
    output_type: Any = Any
    version: int = 1

    def __post_init__(self) -> None:
        name(self.name)
        if type(self.version) is not int or self.version < 1:
            raise ValueError("Contract version must be a positive integer")

    @property
    def key(self) -> str:
        return f"{self.name}:v{self.version}"

    def descriptor(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "input_schema": schema_id(self.input_type),
            "output_schema": schema_id(self.output_type),
        }


@dataclass(frozen=True)
class WorkflowRef(TaskRef[InputT, OutputT]):
    build_id: str | None = None

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.build_id is not None and (
            not isinstance(self.build_id, str) or not self.build_id or len(self.build_id) > 256
        ):
            raise ValueError("build_id must be a nonempty string of at most 256 characters")

    @property
    def manifest(self) -> dict[str, Any]:
        if not self.build_id:
            raise ValueError("A dispatch contract requires an explicit build_id")
        return {
            **self.descriptor(),
            "build_id": self.build_id,
            "replay_version": REPLAY_VERSION,
            "protocol_version": PROTOCOL_VERSION,
        }


@dataclass(frozen=True)
class TopicRef(Generic[InputT]):
    name: str
    payload_type: Any = Any

    def __post_init__(self) -> None:
        if not self.name or len(self.name) > 512 or any(c.isspace() for c in self.name):
            raise ValueError("Invalid topic name")


@dataclass(frozen=True)
class HandlerRef(Generic[InputT, OutputT]):
    task: TaskRef[InputT, OutputT]
    subscription: str

    def __post_init__(self) -> None:
        name(self.subscription)


@dataclass(frozen=True)
class SignalRef(Generic[InputT]):
    name: str
    payload_type: Any = Any

    def __post_init__(self) -> None:
        name(self.name)


@dataclass(frozen=True)
class Deferred:
    """Opaque marker returned by a task delegating its completion."""

    token: str


@dataclass(frozen=True)
class BroadcastBinding:
    topic: TopicRef[Any]
    handler: HandlerRef[Any, Any]


@dataclass(frozen=True)
class WorkflowDefinition:
    ref: WorkflowRef[Any, Any]
    function: Callable[..., Any]
    build_id: str

    @property
    def manifest(self) -> dict[str, Any]:
        return {
            **self.ref.descriptor(),
            "build_id": self.build_id,
            "replay_version": REPLAY_VERSION,
            "protocol_version": PROTOCOL_VERSION,
        }


def workflow(
    *, name: str, version: int = 1, build_id: str | None = None, input_type: Any = None, output_type: Any = None
) -> Callable[[F], F]:
    def decorate(fn: F) -> F:
        if not inspect.iscoroutinefunction(fn):
            raise TypeError("Workflows must be async functions")
        params = list(inspect.signature(fn).parameters.values())
        if len(params) != 2 or any(p.kind not in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) for p in params):
            raise TypeError("Workflow signature must be (context, input)")
        selected = {}
        if input_type is None and params[1].name in fn.__annotations__:
            selected[params[1].name] = fn.__annotations__[params[1].name]
        if output_type is None and "return" in fn.__annotations__:
            selected["return"] = fn.__annotations__["return"]
        try:
            hints = get_type_hints(SimpleNamespace(__annotations__=selected), globalns=fn.__globals__)
        except NameError as exc:
            raise ValueError("Locally scoped payload types require explicit input_type/output_type") from exc
        inp = hints.get(params[1].name, Any) if input_type is None else input_type
        out = hints.get("return", Any) if output_type is None else output_type
        ref: WorkflowRef[Any, Any] = WorkflowRef(name, inp, out, version)
        if build_id is None:
            try:
                source = inspect.getsource(fn)
            except (OSError, TypeError):
                raise ValueError("Dynamic workflows require a stable build_id") from None
            ident = hashlib.sha256(source.encode()).hexdigest()
        else:
            ident = build_id
        if not isinstance(ident, str) or not ident or len(ident) > 256:
            raise ValueError("Invalid workflow build_id")
        setattr(fn, "__duraflow_workflow__", WorkflowDefinition(ref, fn, ident))
        return fn

    return decorate


def task(*, ref: TaskRef[Any, Any]) -> Callable[[F], F]:
    def decorate(fn: F) -> F:
        params = list(inspect.signature(fn).parameters.values())
        if len(params) not in (1, 2) or any(p.kind not in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD) for p in params):
            raise TypeError("Task signature must be (input) or (context, input)")
        ref.descriptor()
        setattr(fn, "__duraflow_task__", ref)
        return fn

    return decorate


class Registry:
    def __init__(self, *functions: Callable[..., Any]):
        self.workflows: dict[str, WorkflowDefinition] = {}
        self.tasks: dict[str, tuple[TaskRef[Any, Any], Callable[..., Any]]] = {}
        for fn in functions:
            self.register(fn)

    def register(self, fn: Callable[..., Any]) -> None:
        definition = getattr(fn, "__duraflow_workflow__", None)
        ref = getattr(fn, "__duraflow_task__", None)
        if definition is not None:
            if definition.ref.key in self.workflows:
                raise Conflict(f"Duplicate workflow {definition.ref.key}")
            self.workflows[definition.ref.key] = definition
        elif ref is not None:
            if ref.key in self.tasks:
                raise Conflict(f"Duplicate task {ref.key}")
            self.tasks[ref.key] = (ref, fn)
        else:
            raise TypeError("Register a @workflow or @task function")

    def resolve(self, ref: Any) -> WorkflowDefinition:
        if callable(ref):
            ref = getattr(ref, "__duraflow_workflow__").ref
        key = ref.key if isinstance(ref, WorkflowRef) else str(ref)
        try:
            return self.workflows[key]
        except KeyError:
            raise WorkflowBlocked(f"Missing workflow implementation: {key}") from None

    def match_manifest(self, manifest: dict[str, Any]) -> WorkflowDefinition | None:
        definition = self.workflows.get(f"{manifest.get('name')}:v{manifest.get('version')}")
        if definition is None or definition.manifest != manifest:
            return None
        return definition


class Clock:
    def now(self) -> float:
        return datetime.now(timezone.utc).timestamp()


class ManualClock(Clock):
    def __init__(self, value: float = 1_700_000_000.0):
        self.value = value

    def now(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        if not math.isfinite(seconds) or seconds < 0:
            raise ValueError("Clock cannot move backwards")
        self.value += seconds


def clock_now(clock: Clock) -> float:
    """Server time inside atomic store mutations; the configured clock otherwise."""
    trusted = _STORE_TIME.get()
    return clock.now() if trusted is None else trusted
