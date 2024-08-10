import datetime as dt

import pytest
from dateutil.parser import ParserError
from zyp.function import to_datetime, to_unixtime

stddate = dt.datetime(2023, 6, 30)


def test_to_datetime_success():
    assert to_datetime("06/30/2023") == stddate
    assert to_datetime("06/05/2023") == dt.datetime(2023, 6, 5)
    assert to_datetime(stddate) == stddate
    assert to_datetime("---") is None
    assert to_datetime(None) is None


def test_to_datetime_failure():
    with pytest.raises(ParserError) as ex:
        to_datetime("---", on_error="raise")
    assert ex.match("String does not contain a date: ---")


def test_to_unixtime_success():
    assert to_unixtime("06/30/2023") == 1688076000.0
    assert to_unixtime("06/05/2023") == 1685916000.0
    assert to_unixtime(stddate) == 1688076000.0
    assert to_unixtime("---") is None
    assert to_unixtime(123) == 123
    assert to_unixtime(123.45) == 123.45
    assert to_unixtime(None) is None


def test_to_unixtime_failure():
    with pytest.raises(ParserError) as ex:
        to_unixtime("---", on_error="raise")
    assert ex.match("String does not contain a date: ---")

    with pytest.raises(ValueError) as ex:
        to_unixtime(None, on_error="raise")
    assert ex.match("Converting value to unixtime failed: None")
