import re

import pytest
from jmespath.exceptions import ParseError

from zyp.model.moksha import MokshaRule, MokshaTransformation


def test_moksha_rule_compile_success():
    rule = MokshaRule(type="jmes", expression="@")
    moksha = rule.compile()
    assert moksha.transformer.expression == "@"
    assert moksha.transformer.parsed == {"type": "current", "children": []}


def test_moksha_rule_compile_syntax_error_jmes():
    with pytest.raises(ParseError) as ex:
        MokshaRule(type="jmes", expression="@foo").compile()
    assert ex.match("Unexpected token: foo")


def test_moksha_rule_compile_syntax_error_jq():
    with pytest.raises(ValueError) as ex:
        MokshaRule(type="jq", expression="foo").compile()
    assert ex.match("jq: error: foo/0 is not defined at <top-level>, line 1")


def test_moksha_rule_evaluate_success_jmes():
    rule = MokshaRule(type="jmes", expression="@")
    assert rule.compile().evaluate(42.42) == 42.42


def test_moksha_rule_evaluate_success_jq():
    rule = MokshaRule(type="jq", expression=".")
    assert rule.compile().evaluate(42.42) == 42.42


def test_moksha_rule_evaluate_invalid_transformer():
    rule = MokshaRule(type="jmes", expression="@")
    compiled = rule.compile()
    compiled.transformer = "foo"
    with pytest.raises(TypeError) as ex:
        compiled.evaluate(42.42)
    assert ex.match("Evaluation failed. Type must be either jmes or jq or transon: foo")


def test_moksha_transformation_success_jq():
    moksha = MokshaTransformation().jq(". /= 100")
    assert moksha.apply(4242) == 42.42


def test_moksha_transformation_error_jq_scalar(caplog):
    moksha = MokshaTransformation().jq(". /= 100")
    with pytest.raises(ValueError) as ex:
        moksha.apply("foo")
    assert ex.match(re.escape('string ("foo") and number (100) cannot be divided'))

    assert "Error evaluating rule: MokshaRuntimeRule(type='jq'" in caplog.text
    assert "Error payload:\nfoo" in caplog.messages


def test_moksha_transformation_error_jq_map(caplog):
    moksha = MokshaTransformation().jq(".foo")
    with pytest.raises(ValueError) as ex:
        moksha.apply(map(lambda x: x, ["foo"]))  # noqa: C417
    assert ex.match(re.escape('Cannot index array with string "foo"'))

    assert "Error evaluating rule: MokshaRuntimeRule(type='jq'" in caplog.text
    assert "Error payload:\n[]" in caplog.messages


def test_moksha_transformation_empty():
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
