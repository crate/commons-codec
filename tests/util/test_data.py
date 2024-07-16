from decimal import Decimal

from commons_codec.util.data import is_number


def test_is_number_numeric():
    assert is_number(42)
    assert is_number(42.42)
    assert is_number(-42)
    assert is_number(-42.42)
    assert is_number("42")
    assert is_number("42.42")
    assert is_number("-42")
    assert is_number("-42.42")
    assert is_number(Decimal("42"))
    assert is_number(Decimal("42.42"))
    assert is_number(Decimal("-42"))
    assert is_number(Decimal("-42.42"))
    # https://stackoverflow.com/q/45923675
    assert is_number("1١¼Ⅱ¼")


def test_is_number_non_numeric():
    assert not is_number("abc")
    assert not is_number("🌻")
    assert not is_number({})
    assert not is_number([])
    assert not is_number(object())
