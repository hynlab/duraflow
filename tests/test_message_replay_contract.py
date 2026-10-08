"""Replay rejects incompatible versions and divergence from completed protocol-2 history."""

from copy import deepcopy

import pytest

from duraflow import Registry, TaskRef, WorkflowContext, workflow
from duraflow.contracts import NonDeterminism, ProtocolError, UnsupportedWorkflow
from duraflow.testing import TestEnvironment
from duraflow.workflow_replay import execute
from tests.test_engine import double, sequence


@pytest.mark.parametrize("change", ["codec", "protocol", "build", "return", "command", "exception", "budget"])
async def test_incompatible_replay_cannot_silently_rewrite_committed_history(change):
    async with TestEnvironment(Registry(sequence, double)) as env:
        handle = await env.client.start(sequence, 3, request_id="history")
        assert await env.run(handle) == 13
        original = await handle.describe()
        state = deepcopy(original)
        definition = env.client.registry.resolve(sequence)
        options = {}
        expected = NonDeterminism
        if change in {"codec", "protocol"}:
            state["codec_version" if change == "codec" else "execution_protocol"] = 999
            expected = ProtocolError
        elif change == "build":
            state["manifest"]["build_id"] = "different"
        elif change == "budget":
            options["max_steps"] = 1
            expected = UnsupportedWorkflow
        else:

            @workflow(name="sequence", build_id="test-v1")
            async def changed(ctx: WorkflowContext, value: int) -> int:
                if change == "exception":
                    raise ValueError("unexpected")
                if change == "command":
                    return await ctx.call(TaskRef("double", int, int), value + 1)
                return value

            definition = Registry(changed).resolve(changed)
        with pytest.raises(expected):
            execute(definition, state, **options)
        assert await handle.result(timeout=1) == 13
        assert (await handle.describe())["commands"] == original["commands"]
