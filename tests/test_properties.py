"""Generated invariants, enabled by the dev extra."""

import pytest

pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from duraflow.contracts import fingerprint
from duraflow.state import finish_node


@given(st.lists(st.integers(min_value=0, max_value=100), min_size=1, max_size=100))
def test_terminal_outcome_immutable(values: list[int]) -> None:
    state = {"sequence": 0, "history": []}
    node = {"id": "0.0", "state": "pending"}
    for value in values:
        finish_node(state, node, 0, result=value)
    assert node["result"] == values[0]
    assert state["sequence"] == 1


@given(st.dictionaries(st.text(min_size=1, max_size=20), st.integers(), max_size=20))
def test_mapping_fingerprint_stable(data: dict[str, int]) -> None:
    assert fingerprint(data) == fingerprint(dict(reversed(list(data.items()))))
