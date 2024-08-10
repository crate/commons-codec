import pytest
from jsonpointer import JsonPointer, JsonPointerException
from zyp.util.locator import swap_node


def test_swap_node_not_found_raise():
    data = {"abc": "def"}
    pointer = JsonPointer("/foo")
    with pytest.raises(JsonPointerException) as ex:
        swap_node(pointer, data, on_error="raise")
    assert ex.match("Element not found: /foo")


def test_swap_node_not_found_ignore():
    data = {"abc": "def"}
    pointer = JsonPointer("/foo")
    new_data = swap_node(pointer, data, on_error="ignore")
    assert new_data is data
