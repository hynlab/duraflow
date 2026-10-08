"""Contracts used by clients; this module imports no application implementations."""

from duraflow import ChannelRef, WorkflowRef

APPROVAL = ChannelRef("approval", bool)
ORDER = WorkflowRef("approval-example", int, int, build_id="signals-v1")
workflows = {"approval-example:v1": ORDER}
signals = {"approval": APPROVAL}
