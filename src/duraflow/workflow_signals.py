"""Signal admission and mailbox delivery within a workflow transaction."""

from typing import Any

from .channels import SignalFilter
from .contracts import Conflict, ProtocolError, TERMINAL, fingerprint
from .message_records import Records
from .retention import signal_expiry
from .state import event


def deliver_signal(stream: dict[str, Any], body: dict[str, Any], signal_id: str) -> bool:
    if stream["channel"] != body["channel"] or stream["schema"] != body["schema"]:
        return False
    if stream["accepted_schema"] not in {stream["schema"], body.get("payload_schema")}:
        return False
    if stream["max_signals"] is not None and stream["accepted"] >= stream["max_signals"]:
        return False
    if not SignalFilter(stream["filter"]).matches(body["payload"]):
        return False
    if len(stream["received"]) >= 1000:
        raise ProtocolError("Channel buffer capacity exceeded")
    stream["accepted"] += 1
    stream["received"].append(
        {"id": signal_id, "payload": body["payload"], "consumed": False, "payload_schema": body.get("payload_schema")}
    )
    return True


def accept_signal(
    state: dict[str, Any],
    aggregate: dict[str, Any],
    records: Records | None,
    body: dict[str, Any],
    now: float,
    horizon: float,
) -> tuple[dict[str, int] | None, str | None]:
    expires_at = None
    if "signal_epoch" in body:
        try:
            expires_at = signal_expiry(body["signal_epoch"], now, horizon)
        except Conflict:
            return None, "SignalExpired"
    record_key = "signal/" + fingerprint([body.get("signal_epoch"), body["signal_id"]])
    digest = fingerprint([body["channel"], body["schema"], body.get("payload_schema"), body["payload"]])
    previous_record = records.get(record_key) if records is not None else None
    previous = previous_record["digest"] if previous_record else state["signal_keys"].get(body["signal_id"])
    if previous is not None:
        if previous_record and previous_record.get("legacy_digest"):
            digest = fingerprint([body["channel"], body["schema"], body["payload"]])
        return None, "Conflict" if previous != digest else None
    if state["status"] in TERMINAL:
        return None, "Conflict"
    if records is not None:
        if expires_at is None:
            if aggregate.get("legacy_signal_count", 0) >= 10000:
                return None, "SignalLimit"
            aggregate["legacy_signal_count"] = aggregate.get("legacy_signal_count", 0) + 1
        records.put(record_key, {"digest": digest}, expires_at=expires_at)
    else:
        state["signal_keys"][body["signal_id"]] = digest
    accepted = sum(deliver_signal(stream, body, body["signal_id"]) for stream in state["channels"].values())
    event(
        state,
        "signal_received" if accepted else "signal_discarded",
        now,
        signal_id=body["signal_id"],
        channel=body["channel"],
    )
    return {"receivers": accepted}, None
