"""
Exercise a few transformation recipes using `jq`.
https://github.com/jqlang/jq/blob/master/src/builtin.jq
"""

from copy import deepcopy

import pytest
from jmespath.exceptions import ParseError

from zyp.model.moksha import MokshaRule, MokshaTransformation


def test_moksha_jq_idempotency():
    """
    Idempotent transformations should not modify data.
    """
    data = [{"foo": "bar"}, {"baz": "qux"}]
    transformation = MokshaTransformation().jq(".")
    assert transformation.apply(deepcopy(data)) == data


def test_moksha_jq_select_pick_keys():
    """
    Verify selecting elements with moksha/jq.
    """
    data_in = [{"meta": {"id": "Hotzenplotz", "timestamp": 123456789}, "data": {"abc": 123, "def": 456}}]
    data_out = [{"meta": {"id": "Hotzenplotz"}, "data": {"abc": 123}}]
    transformation = MokshaTransformation().jq(".[] |= pick(.meta.id, .data.abc)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_pick_indices():
    """
    Verify selecting elements with moksha/jq.
    """
    data_in = [{"data": [1, {"foo": "bar"}, 2]}]
    data_out = [{"data": [1, 2]}]
    transformation = MokshaTransformation().jq(".[] |= (pick(.data.[0], .data.[2]) | prune_null)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_drop_keys():
    """
    Verify selecting elements with moksha/jq.
    """
    data_in = [{"meta": {"id": "Hotzenplotz", "timestamp": 123456789}, "data": {"abc": 123, "def": 456}}]
    data_out = [{"meta": {"id": "Hotzenplotz"}, "data": {"abc": 123}}]
    transformation = MokshaTransformation().jq(".[] |= del(.meta.timestamp, .data.def)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_drop_indices():
    """
    Verify selecting elements with moksha/jq.
    """
    data_in = [{"data": [1, {"foo": "bar"}, 2]}]
    data_out = [{"data": [1, 2]}]
    transformation = MokshaTransformation().jq(".[] |= del(.data.[1])")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_compute_scalar():
    """
    Verify updating deeply nested field with value.
    https://stackoverflow.com/a/65822084
    """
    data_in = [{"data": {"abc": 123}}]
    data_out = [{"data": {"abc": 246}}]
    transformation = MokshaTransformation().jq(".[] |= (.data.abc *= 2)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_cast_string():
    """
    Verify type casting using moksha/jq.
    """
    data_in = [{"data": {"abc": 123}}]
    data_out = [{"data": {"abc": "123"}}]
    transformation = MokshaTransformation().jq(".[] |= (.data.abc |= tostring)")
    assert transformation.apply(data_in) == data_out

    data_in = [{"data": [{"abc": 123}, {"abc": "123"}]}]
    data_out = [{"data": [{"abc": "123"}, {"abc": "123"}]}]
    transformation = MokshaTransformation().jq(".[] |= (.data[].abc |= tostring)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_cast_array_exact():
    """
    Verify type casting using moksha/jq.
    """
    transformation = MokshaTransformation().jq(".[] |= (.data.abc |= to_array)")
    assert transformation.apply([{"data": {"abc": 123}}]) == [{"data": {"abc": [123]}}]
    assert transformation.apply([{"data": {"abc": [123]}}]) == [{"data": {"abc": [123]}}]


def test_moksha_jq_cast_array_iterable():
    """
    Verify type casting using moksha/jq.
    """

    data_in = [{"data": [{"abc": 123}, {"abc": [456]}]}]
    data_out = [{"data": [{"abc": [123]}, {"abc": [456]}]}]

    transformation = MokshaTransformation().jq(".[] |= (.data[].abc |= to_array)")
    assert transformation.apply(data_in) == data_out

    transformation = MokshaTransformation().jq(".[] |= (.data[] |= (.abc |= to_array))")
    assert transformation.apply(data_in) == data_out

    transformation = MokshaTransformation().jq(".[] |= (.data[].abc |= (foreach . as $item (0; $item; to_array)))")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_cast_dict():
    """
    Verify type casting using moksha/jq.
    """

    data_in = [{"data": [{"abc": 123}, {"abc": 456}, {"abc": {"id": 789}}]}]
    data_out = [{"data": [{"abc": {"id": 123}}, {"abc": {"id": 456}}, {"abc": {"id": 789}}]}]

    transformation = MokshaTransformation().jq('.[] |= (.data[].abc |= to_object({"key": "id"}))')
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_prune_array_of_objects():
    """
    Verify dropping arrays of objects recursively.
    """
    data_in = [{"data": {"abc": [{"foo": 1}], "def": [42.42]}}]
    data_out = [{"data": {"abc": None, "def": [42.42]}}]
    transformation = MokshaTransformation().jq(".[] |= prune_array_of_objects")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_flatten_array():
    """
    Verify flattening nested arrays.
    """
    data_in = [{"data": {"abc": [{"foo": 1}, [{"foo": 2}, {"foo": 3}]]}}]
    data_out = [{"data": {"abc": [{"foo": 1}, {"foo": 2}, {"foo": 3}]}}]
    transformation = MokshaTransformation().jq(".[] |= (.data.abc |= flatten)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_rule_disabled():
    assert MokshaRule(type="jq", expression=". | tostring", disabled=True).compile().evaluate(42.42) == 42.42


def test_transon_duplicate_records():
    """
    Verify record duplication works well.
    """
    data_in = [{"foo": "bar", "baz": "qux"}]
    data_out = [{"foo": "bar", "baz": "qux"}] * 42
    transformation = MokshaTransformation().transon({"$": "expr", "op": "mul", "value": 42})
    assert transformation.apply(data_in) == data_out


def test_transon_idempotency():
    """
    Idempotent transformations should not modify data.
    """
    data = [{"foo": "bar"}, {"baz": "qux"}]
    transformation = MokshaTransformation().transon({"$": "this"})
    assert transformation.apply(deepcopy(data)) == data


def test_moksha_rule():
    moksha = MokshaRule(type="jmes", expression="@").compile()
    assert moksha.transformer.expression == "@"
    assert moksha.transformer.parsed == {"type": "current", "children": []}


def test_moksha_runtime_rule_success():
    assert MokshaRule(type="jmes", expression="@").compile().evaluate(42.42) == 42.42


def test_moksha_runtime_rule_syntax_error():
    with pytest.raises(ParseError) as ex:
        MokshaRule(type="jmes", expression="@foo").compile()
    assert ex.match("Unexpected token: foo")


def test_moksha_runtime_rule_invalid_transformer():
    rule = MokshaRule(type="jmes", expression="@").compile()
    rule.transformer = "foo"
    with pytest.raises(TypeError) as ex:
        rule.evaluate(42.42)
    assert ex.match("Evaluation failed. Type must be either jmes or jq or transon: foo")


def test_moksha_empty():
    """
    Empty JSON Pointer expression means "root node".
    """
    with pytest.raises(ValueError) as ex:
        MokshaTransformation().jmes("")
    assert ex.match("JMESPath expression cannot be empty")

    with pytest.raises(ValueError) as ex:
        MokshaTransformation().jq("")
    assert ex.match("jq expression cannot be empty")

    with pytest.raises(ValueError) as ex:
        MokshaTransformation().transon("")
    assert ex.match("transon expression cannot be empty")
