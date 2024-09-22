from copy import deepcopy

from zyp.model.moksha import MokshaTransformation


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
