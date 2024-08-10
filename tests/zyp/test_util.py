import jmespath
import jq
import jsonpointer
import pytest
import transon
from zyp.util.expression import compile_expression
from zyp.util.locator import to_pointer


def test_to_pointer_string():
    assert to_pointer("/") == jsonpointer.JsonPointer("/")
    assert to_pointer("") == jsonpointer.JsonPointer("")


def test_to_pointer_jsonpointer():
    assert to_pointer(jsonpointer.JsonPointer("/")) == jsonpointer.JsonPointer("/")


def test_to_pointer_none():
    with pytest.raises(TypeError) as ex:
        to_pointer(None)
    assert ex.match("Value is not of type str or JsonPointer: NoneType")


def test_to_pointer_int():
    with pytest.raises(TypeError) as ex:
        to_pointer(42)
    assert ex.match("Value is not of type str or JsonPointer: int")


def test_compile_expression_jmes():
    transformer: jmespath.parser.ParsedResult = compile_expression(type="jmes", expression="@")
    assert transformer.expression == "@"
    assert transformer.parsed == {"type": "current", "children": []}


def test_compile_expression_jq():
    transformer: jq._Program = compile_expression(type="jq", expression=".")
    assert transformer.program_string == "."


def test_compile_expression_transon():
    transformer: transon.Transformer = compile_expression(type="transon", expression={"$": "this"})
    assert transformer.template == {"$": "this"}


def test_compile_expression_unknown():
    with pytest.raises(TypeError) as ex:
        compile_expression(type="foobar", expression=None)
    assert ex.match("Compilation failed. Type must be either jmes or jq or transon: foobar")
