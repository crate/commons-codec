import pytest
from jmespath.exceptions import ParseError
from zyp.model.moksha import MokshaRule, MokshaTransformation


def test_moksha_jq_compute_nested():
    """
    Verify updating deeply nested field with value, using moksha/jq.
    https://stackoverflow.com/a/65822084
    """
    transformation = MokshaTransformation().jq(".[] |= (.data.abc *= 2)")
    assert transformation.apply([{"data": {"abc": 123}}]) == [{"data": {"abc": 246}}]


def test_transon_duplicate_records():
    """
    Verify record duplication works well.
    """
    transformation = MokshaTransformation().transon({"$": "expr", "op": "mul", "value": 42})
    assert transformation.apply([{"foo": "bar", "baz": "qux"}]) == [{"foo": "bar", "baz": "qux"}] * 42


def test_transon_idempotency():
    """
    Verify record duplication works well.
    """
    transformation = MokshaTransformation().transon({"$": "this"})
    assert transformation.apply([{"foo": "bar"}, {"baz": "qux"}]) == [{"foo": "bar"}, {"baz": "qux"}]


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
