"""Versioned messages and topic routing for the event-driven runtime."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import re
from typing import Any
from uuid import uuid4

from .contracts import ProtocolError, canonical, name, parse_json

MAX_MESSAGE_BYTES = 16 * 1024 * 1024


class RetryLater(Exception):
    """A valid delivery cannot yet be consumed; retain it through broker redelivery."""


@dataclass(frozen=True)
class Message:
    kind: str
    key: str
    body: dict[str, Any]
    id: str = field(default_factory=lambda: str(uuid4()))
    version: int = 2

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str):
            raise ProtocolError("Message kind must be a string")
        name(self.kind)
        if (
            type(self.version) is not int
            or self.version != 2
            or not isinstance(self.id, str)
            or not self.id
            or len(self.id) > 128
            or not isinstance(self.key, str)
            or not self.key
            or len(self.key) > 1024
        ):
            raise ProtocolError("Invalid message identity or protocol")
        if not isinstance(self.body, dict):
            raise ProtocolError("Message body must be a mapping")

    def require(self, **fields: type) -> None:
        for key, typ in fields.items():
            value = self.body.get(key)
            if not isinstance(value, typ) or (typ is int and type(value) is not int):
                raise ProtocolError(f"Invalid message field: {key}")

    def to_bytes(self) -> bytes:
        raw = canonical(asdict(self)).encode()
        if len(raw) > MAX_MESSAGE_BYTES:
            raise ProtocolError("Message exceeds 16 MiB")
        return raw

    @classmethod
    def from_bytes(cls, raw: bytes) -> Message:
        if len(raw) > MAX_MESSAGE_BYTES:
            raise ProtocolError("Message exceeds 16 MiB")
        try:
            value = parse_json(raw)
            if set(value) != {"kind", "key", "body", "id", "version"}:
                raise ProtocolError("Invalid message envelope")
            return cls(**value)
        except (TypeError, ValueError, AttributeError) as exc:
            raise ProtocolError("Invalid message envelope") from exc


@dataclass(frozen=True)
class Topics:
    """Physical Pulsar tenancy is separate from logical application namespaces."""

    namespace: str = "default"
    tenant: str = "public"
    environment: str = "default"

    def __post_init__(self) -> None:
        for value in (self.namespace, self.tenant, self.environment):
            name(value)

    def topic(self, role: str, entity: str) -> str:
        name(role)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,255}", entity):
            raise ValueError("Invalid topic entity")
        return (
            f"persistent://{self.tenant}/{self.environment}/df2-{len(self.namespace)}-{self.namespace}-{role}-{entity}"
        )

    def workflow(self, workflow: str) -> str:
        return self.topic("workflow", workflow)

    def commands(self, workflow: str) -> str:
        return self.topic("command", workflow)

    def control(self, workflow: str) -> str:
        return self.topic("control", workflow)

    def replay(self, workflow: str, build: str) -> str:
        from .contracts import fingerprint

        return self.topic("replay", workflow + "-" + fingerprint(build)[:16])

    def task(self, task: str, version: int) -> str:
        return self.topic("task", f"{task}-v{version}")

    def task_control(self, task: str, version: int) -> str:
        return self.topic("task-control", f"{task}-v{version}")

    def task_completion(self, task: str, version: int) -> str:
        return self.topic("task-completion", f"{task}-v{version}")

    def task_tags(self, *, control: bool = False) -> str:
        return self.topic("task-tag-control" if control else "task-tags", "tasks")

    def tags(self) -> str:
        return self.topic("tags", "workflows")

    def reply(self, client: str) -> str:
        return self.topic("reply", client)

    def instance(self, workflow: str, workflow_id: str) -> str:
        return canonical([self.namespace, workflow, workflow_id])


@dataclass(frozen=True)
class Publication:
    topic: str
    message: Message
    deliver_at: float | None = None
    subscription: str | None = None

    def document(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def restore(cls, value: dict[str, Any]) -> Publication:
        return cls(**{**value, "message": Message(**value["message"])})
