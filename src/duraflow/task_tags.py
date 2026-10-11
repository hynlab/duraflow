"""Service-tag engine: a separate keyed index and resumable cancellation fan-out."""

from __future__ import annotations

from typing import Any

from .contracts import ProtocolError, fingerprint
from .message_runtime import Consumer, OutboxRelay
from .message_store import MessageStore
from .message_records import Records, RecordUpdate
from .messaging import Message, Publication, Topics
from .state import identity
from .transport import Delivery, Transport


class TaskTagEngine(Consumer):
    def __init__(
        self, store: MessageStore, transport: Transport, *, topics: Topics | None = None, page_size: int = 100
    ):
        super().__init__(transport, topics or Topics())
        if not 1 <= page_size <= 5000:
            raise ValueError("Invalid fan-out page size")
        self.store, self.page_size = store, page_size
        self.routes = [(self.topics.task_tags(), "tags", True), (self.topics.task_tags(control=True), "tags", True)]
        self.relay = OutboxRelay(store, transport)

    async def handle(self, message: Message, delivery: Delivery) -> None:
        body = message.body
        if message.kind == "task_tag_cancel" and delivery.topic != self.topics.task_tags(control=True):
            raise ProtocolError("Task tag controls require the operator topic")

        def update(state: dict[str, Any], now: float, records: Records) -> list[Publication]:
            members, closed = state.setdefault("members", {}), state.get("closed", {})
            if message.kind == "task_tag_add":
                if body["dispatch_id"] not in closed and records.get("closed/" + body["dispatch_id"]) is None:
                    members[body["dispatch_id"]] = body["topic"]
                return []
            if message.kind == "task_tag_remove":
                members.pop(body["dispatch_id"], None)
                if body["dispatch_id"] not in closed:
                    record_key = "closed/" + body["dispatch_id"]
                    previous = records.get(record_key)
                    expires_at = message.expires_at()
                    if previous is not None:
                        old_expiry = previous.get("expires_at")
                        expires_at = None if expires_at is None or old_expiry is None else max(expires_at, old_expiry)
                    records.put(record_key, {"closed": True, "expires_at": expires_at}, expires_at=expires_at)
                return []
            if message.kind != "task_tag_cancel":
                raise ProtocolError("Unexpected task tag event")
            job_id = body["request_id"]
            digest = fingerprint({key: value for key, value in body.items() if key not in {"offset", "_df_expires_at"}})
            job = records.get("job/" + job_id)
            if job is None:
                old_jobs = state.get("jobs", {})
                job = old_jobs.pop(job_id, None)
                if job is None and state.get("job_count", 0) + len(old_jobs) >= 10000:
                    raise ProtocolError("Task-tag request quota exceeded")
                state["job_count"] = state.get("job_count", 0) + 1
                job = job or {
                    "digest": digest,
                    "targets": sorted(members.items()),
                    "offset": 0,
                    "expires_at": message.expires_at(),
                }
                records.put("job/" + job_id, job)
            if job["digest"] != digest:
                raise ProtocolError("Task tag request identity conflict")
            if job.get("complete") or body.get("offset", 0) != job["offset"]:
                return []
            targets = job["targets"][job["offset"] : job["offset"] + self.page_size]
            publications = [
                Publication(
                    topic,
                    Message(
                        "cancel_task",
                        dispatch_id,
                        {"dispatch_id": dispatch_id, "_df_expires_at": job.get("expires_at")},
                        id=identity(message.id, dispatch_id),
                    ),
                    subscription="tasks",
                )
                for dispatch_id, topic in targets
            ]
            job["offset"] += len(targets)
            if job["offset"] < len(job["targets"]):
                continuation = Message(
                    "task_tag_cancel", message.key, {**body, "offset": job["offset"]}, id=identity(message.id, "next")
                )
                publications.append(Publication(self.topics.task_tags(control=True), continuation, subscription="tags"))
            else:
                job["complete"], job["targets"] = True, []
            records.put("job/" + job_id, job)
            return publications

        await self.store.apply("task-tag/" + message.key, message, RecordUpdate(update))

    async def step(self) -> bool:
        sent = await self.relay.step()
        return bool(await super().step() or sent)
