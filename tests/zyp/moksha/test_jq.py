"""
Exercise a few transformation recipes using `jq`.
https://github.com/jqlang/jq/blob/master/src/builtin.jq
"""

from copy import deepcopy

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
    Verify selecting elements with moksha/jq, by picking individual keys from objects.
    """
    data_in = [{"meta": {"id": "Hotzenplotz", "timestamp": 123456789}, "data": {"abc": 123, "def": 456}}]
    data_out = [{"meta": {"id": "Hotzenplotz"}, "data": {"abc": 123}}]
    transformation = MokshaTransformation().jq(".[] |= pick(.meta.id, .data.abc)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_pick_indices():
    """
    Verify selecting elements with moksha/jq, by picking individual indices from arrays.
    """
    data_in = [{"data": [1, {"foo": "bar"}, 2]}]
    data_out = [{"data": [1, 2]}]
    transformation = MokshaTransformation().jq(".[] |= pick(.data.[0], .data.[2]) | prune_null")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_drop_keys_object_exact():
    """
    Verify selecting elements with moksha/jq, by dropping individual keys from objects.
    """
    data_in = [{"meta": {"id": "Hotzenplotz", "timestamp": 123456789}, "data": {"abc": 123, "def": 456}}]
    data_out = [{"meta": {"id": "Hotzenplotz"}, "data": {"abc": 123}}]
    transformation = MokshaTransformation().jq(".[] |= del(.meta.timestamp, .data.def)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_drop_keys_object_iterable():
    """
    Verify selecting elements with moksha/jq, by dropping individual keys from objects within arrays.
    In this case, on some documents, the array isn't present at all.
    """
    data_in = [
        {"data": {"array": [{"abc": 123, "def": 456}, {"abc": 123, "def": 456}, {"abc": 123}]}},
        {"data": {"array": 42}},
        {"data": {}},
        {"meta": {"version": 42}},
    ]
    data_out = [
        {"data": {"array": [{"abc": 123}, {"abc": 123}, {"abc": 123}]}},
        {"data": {"array": 42}},
        {"data": {}},
        {"meta": {"version": 42}},
    ]
    transformation = MokshaTransformation().jq(".[] |= del(.data.array[]?.def)")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_select_drop_indices():
    """
    Verify selecting elements with moksha/jq, by dropping individual indices from arrays.
    """
    data_in = [{"data": [1, {"foo": "bar"}, 2]}]
    data_out = [{"data": [1, 2]}]
    transformation = MokshaTransformation().jq(".[] |= del(.data.[1])")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_compute_scalar_exact():
    """
    Verify updating value of deeply nested attribute if it exists.
    https://stackoverflow.com/a/65822084
    """
    data_in = [
        {"data": {"abc": 123}},
        {"data": {"def": 456}},
        {"meta": {"version": 42}},
    ]
    data_out = [
        {"data": {"abc": 246}},
        {"data": {"def": 456}},
        {"meta": {"version": 42}},
    ]
    transformation = MokshaTransformation().jq(".[] |= if .data.abc then .data.abc *= 2 end")
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_compute_scalar_iterable():
    """
    Verify updating value of deeply nested attribute within an array if it exists.
    """
    data_in = [
        {"data": [{"abc": 123}]},
        {"data": [{"def": 456}]},
        {"data": None},
        {"data": 42},
        {"meta": {"version": 42}},
    ]
    data_out = [
        {"data": [{"abc": 246}]},
        {"data": [{"def": 456}]},
        {"data": None},
        {"data": 42},
        {"meta": {"version": 42}},
    ]
    transformation = MokshaTransformation().jq(
        '.[] |= if (.data | type == "array") and .data[].abc then .data[].abc *= 2 end'
    )
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


def test_moksha_jq_cast_object_naive():
    """
    Lists are hard. Verify type casting using jqlang stdlib's `to_object` on array of objects.

    The naive approach will manifest substructures even on documents
    that don't include the relevant attribute at all.
    """

    data_in = [
        {
            "data": [
                {"abc": 123},
                {"abc": 456},
                {"abc": {"id": 789}},
                {},
                {"def": 999},
            ]
        },
    ]
    data_out = [
        {
            "data": [
                {"abc": {"id": 123}},
                {"abc": {"id": 456}},
                {"abc": {"id": 789}},
                {},
                {"def": 999},
            ]
        },
    ]

    transformation = MokshaTransformation().jq('.[] |= (.data[].abc |= to_object({"key": "id", "zap": true}))')
    assert transformation.apply(data_in) == data_out


def test_moksha_jq_cast_object_advanced():
    """
    Lists are hard. Verify type casting using jqlang stdlib's `to_object` on array of objects.

    The advanced approach will manifest substructures only on documents
    that include the relevant attribute.
    """

    data_in = [
        {
            "data": [
                {"abc": 123},
                {"abc": 456},
                {"abc": {"id": 789}},
                {},
                {"def": 999},
            ]
        },
    ]
    data_out = [
        {
            "data": [
                {"abc": {"id": 123}},
                {"abc": {"id": 456}},
                {"abc": {"id": 789}},
                {},
                {"def": 999},
            ]
        },
    ]

    transformation = MokshaTransformation().jq(
        '.[] |= (.data[] |= (if .abc then .abc |= to_object({"key": "id"}) end))'
    )
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
