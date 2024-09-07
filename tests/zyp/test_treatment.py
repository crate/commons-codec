from zyp.model.treatment import Treatment

RECORD_IN = {
    "data": {
        "ignore_complex_list": [{}],
        "ignore_field": 123,
        "invalid_date_scalar": 123,
        "invalid_date_nested": {"date": "123"},
        "to_string": 123,
        "to_list": 123,
        "to_dict": 123,
    },
}

RECORD_OUT = {
    "data": {
        "to_string": "123",
        "to_list": [123],
        "to_dict": {"id": 123},
    },
}


def test_treatment_all():
    """
    Verify treating nested data.
    """
    transformation = Treatment(
        ignore_complex_lists=True,
        ignore_field=["ignore_field"],
        prune_invalid_date=["invalid_date_scalar", "invalid_date_nested"],
        convert_dict=[{"name": "to_dict", "wrapper_name": "id"}],
        convert_list=["to_list"],
        convert_string=["to_string"],
    )
    assert transformation.apply(RECORD_IN) == RECORD_OUT


def test_treatment_noop():
    """
    Treating nested data without rules will yield the same result.
    """
    transformation = Treatment()
    assert transformation.apply([{"data": {"abc": 123}}]) == [{"data": {"abc": 123}}]


def test_treatment_ignore_complex_lists_basic():
    """
    Verify the "ignore_complex_lists" directive works.
    """
    transformation = Treatment(ignore_complex_lists=True)
    assert transformation.apply([{"data": [{"abc": 123}]}]) == [{}]


def test_treatment_ignore_complex_lists_with_specials():
    """
    Verify the "ignore_complex_lists" directive does not remove special encoded fields.
    """
    transformation = Treatment(ignore_complex_lists=True)
    assert transformation.apply([{"data": [{"abc": 123}], "stamps": [{"$date": 123}]}]) == [
        {"stamps": [{"$date": 123}]}
    ]


def test_treatment_ignore_fields():
    """
    Verify ignoring fields works.
    """
    transformation = Treatment(ignore_field=["abc"])
    assert transformation.apply([{"data": [{"abc": 123}]}]) == [{"data": [{}]}]


def test_treatment_normalize_complex_lists_success():
    """
    Verify normalizing lists of objects works.
    """
    transformation = Treatment(normalize_complex_lists=True)
    assert transformation.apply([{"data": [{"abc": 123.42}, {"abc": 123}]}]) == [
        {"data": [{"abc": 123.42}, {"abc": 123.0}]}
    ]
    assert transformation.apply([{"data": [{"abc": 123}, {"abc": "123"}]}]) == [
        {"data": [{"abc": "123"}, {"abc": "123"}]}
    ]


def test_treatment_normalize_complex_lists_passthrough():
    """
    When no normalization rule can be applied, return input 1:1.
    """
    transformation = Treatment(normalize_complex_lists=True)
    assert transformation.apply([{"data": [{"abc": 123.42}, {"abc": None}]}]) == [
        {"data": [{"abc": 123.42}, {"abc": None}]}
    ]


def test_treatment_convert_string():
    """
    Verify treating nested data to convert values into strings works.
    """
    transformation = Treatment(convert_string=["abc"])
    assert transformation.apply([{"data": [{"abc": 123}]}]) == [{"data": [{"abc": "123"}]}]


def test_treatment_convert_list():
    """
    Verify treating nested data to convert values into lists works.
    """
    transformation = Treatment(convert_list=["abc"])
    assert transformation.apply([{"data": [{"abc": 123}]}]) == [{"data": [{"abc": [123]}]}]


def test_treatment_convert_dict():
    """
    Verify treating nested data to convert values into dicts works.
    """
    transformation = Treatment(convert_dict=[{"name": "abc", "wrapper_name": "id"}])
    assert transformation.apply([{"data": [{"abc": 123}]}]) == [{"data": [{"abc": {"id": 123}}]}]


def test_treatment_prune_invalid_date():
    """
    Verify pruning invalid dates works.
    """
    transformation = Treatment(prune_invalid_date=["date"])
    assert transformation.apply([{"data": [{"date": 123}]}]) == [{"data": [{}]}]
    assert transformation.apply([{"data": [{"date": {"date": 123}}]}]) == [{"data": [{"date": {}}]}]
