"""Persistent paginated signal fan-out by tag; no workflow-state database access."""

from __future__ import annotations

from typing import Any

from .contracts import Conflict, ProtocolError, fingerprint
from .message_runtime import Consumer, OutboxRelay
from .message_store import MessageStore
from .message_records import Records, RecordUpdate
from .retention import signal_expiry
from .messaging import Message, Publication, Topics
from .state import identity
from .transport import Delivery, Transport


class TagEngine(Consumer):
    def __init__(
        self, store: MessageStore, transport: Transport, *, topics: Topics | None = None, page_size: int = 100
    ):
        super().__init__(transport, topics or Topics())
        if not 1 <= page_size <= 5000:
            raise ValueError("Invalid tag page size")
        self.store, self.page_size = store, page_size
        self.routes = [(self.topics.tags(), "tags", True)]
        self.relay = OutboxRelay(store, transport)

    async def handle(self, message: Message, delivery: Delivery) -> None:
        body = message.body
        if message.key != f"{self.topics.namespace}/{body['tag']}":
            raise ProtocolError("Wrong tag identity")

        def update(state: dict[str, Any], now: float, records: Records) -> list[Publication]:
            members = state.setdefault("members", {})
            workflow = body["workflow"]
            ids = members.setdefault(workflow, [])
            if message.kind == "tag_add":
                if body["workflow_id"] not in ids:
                    ids.append(body["workflow_id"])
                    ids.sort()
                response = Message(
                    "tag_registered",
                    body["workflow_key"],
                    {
                        "tag": body["tag"],
                        **({"_df_expires_at": body["_df_expires_at"]} if "_df_expires_at" in body else {}),
                    },
                    id=identity(message.id, "registered"),
                )
                return [Publication(body["reply_to"], response, subscription="state")]
            if message.kind == "tag_remove":
                if body["workflow_id"] in ids:
                    ids.remove(body["workflow_id"])
                return []
            if message.kind != "tag_signal":
                raise ProtocolError("Unexpected tag command")
            try:
                expires_at = signal_expiry(body["signal_epoch"], now) if "signal_epoch" in body else None
            except Conflict as exc:
                raise ProtocolError(str(exc)) from exc
            job_id = (
                identity(workflow, fingerprint([body["signal_epoch"], body["signal_id"]]))
                if "signal_epoch" in body
                else identity(workflow, body["signal_id"])
            )
            digest = fingerprint({key: value for key, value in body.items() if key not in {"offset", "_df_expires_at"}})
            job = records.get(job_id)
            if job is None:
                old_jobs = state.get("jobs", {})
                job = old_jobs.pop(job_id, None)
                if expires_at is None:
                    if job is None and state.get("legacy_job_count", 0) + len(old_jobs) >= 10000:
                        raise ProtocolError("Legacy tag fan-out quota exceeded; use a stable signal epoch")
                    state["legacy_job_count"] = state.get("legacy_job_count", 0) + 1
                job = job or {"digest": digest, "targets": list(ids), "offset": 0, "complete": False}
                records.put(job_id, job, expires_at=expires_at)
            if job["digest"] != digest:
                raise ProtocolError("Tag signal ID reused with different content")
            if job["complete"] or body.get("offset", 0) != job["offset"]:
                return []
            targets = job["targets"][job["offset"] : job["offset"] + self.page_size]
            result = []
            for workflow_id in targets:
                signal = Message(
                    "signal",
                    self.topics.instance(workflow, workflow_id),
                    {
                        key: body[key]
                        for key in (
                            "channel",
                            "schema",
                            "payload_schema",
                            "payload",
                            "signal_id",
                            "signal_epoch",
                            "_df_expires_at",
                        )
                        if key in body
                    },
                    id=identity(job_id, workflow_id),
                )
                result.append(Publication(self.topics.workflow(workflow), signal, subscription="state"))
            job["offset"] += len(targets)
            if job["offset"] < len(job["targets"]):
                continuation = Message(
                    "tag_signal",
                    message.key,
                    {**body, "offset": job["offset"]},
                    id=identity(job_id, "offset/" + str(job["offset"])),
                )
                result.append(Publication(self.topics.tags(), continuation, subscription="tags"))
            else:
                job["complete"], job["targets"] = True, []
            records.put(job_id, job, expires_at=expires_at)
            return result

        await self.store.apply("tag/" + message.key, message, RecordUpdate(update))

    async def step(self) -> bool:
        return bool(await self.relay.step() or await super().step())
