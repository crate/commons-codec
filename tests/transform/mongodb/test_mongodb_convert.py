# ruff: noqa: E402
import pytest

pytestmark = pytest.mark.mongodb

from commons_codec.transform.mongodb import MongoDBCrateDBConverter, date_converter
from zyp.model.bucket import BucketTransformation, ValueConverter
from zyp.model.collection import CollectionTransformation
from zyp.model.treatment import Treatment


def test_date_converter_int():
    """
    Datetime values encoded as integer values will be returned unmodified.
    """
    assert date_converter(1443004362000) == 1443004362000


def test_date_converter_iso8601():
    """
    Datetime values encoded as ISO8601 values will be parsed.
    """
    assert date_converter("2015-09-23T10:32:42.33Z") == 1443004362000
    assert date_converter(b"2015-09-23T10:32:42.33Z") == 1443004362000


def test_date_converter_invalid():
    """
    Incorrect datetime values will not be parsed.
    """
    with pytest.raises(ValueError) as ex:
        date_converter(None)
    assert ex.match("Unable to convert datetime value: None")


def test_convert_basic():
    """
    Just a basic conversion, without transformation.
    """
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "value": {
            "id": 42,
        },
    }
    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "id": 42,
        },
    }

    converter = MongoDBCrateDBConverter()
    assert list(converter.decode_documents([data_in])) == [data_out]


def test_convert_with_treatment_ignore_complex_lists():
    """
    The `ignore_complex_lists` treatment ignores lists of dictionaries, often having deviating substructures.
    """
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "some_complex_list": [
                {"id": "foo", "value": "something"},
                {"id": "bar", "value": {"$date": "2015-09-24T10:32:42.33Z"}},
            ],
        },
    }
    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "id": 42,
            "date": 1443004362000,
        },
    }

    treatment = Treatment(ignore_complex_lists=True)
    transformation = CollectionTransformation(treatment=treatment)
    converter = MongoDBCrateDBConverter(transformation=transformation)
    assert converter.decode_document(data_in) == data_out


def test_convert_with_treatment_normalize_complex_lists():
    """
    The `normalize_complex_lists` treatment converts inner values within lists of dictionaries.
    """
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "some_complex_list": [
                {"id": "foo", "value": "something"},
                {"id": "bar", "value": {"$date": "2015-09-24T10:32:42.33Z"}},
            ],
        },
    }
    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "id": 42,
            "date": 1443004362000,
            "some_complex_list": [
                {"id": "foo", "value": "something"},
                # FIXME: `normalize_complex_lists` does not see it's a timestamp.
                {"id": "bar", "value": "{'$date': '2015-09-24T10:32:42.33Z'}"},
            ],
        },
    }

    treatment = Treatment(normalize_complex_lists=True)
    transformation = CollectionTransformation(treatment=treatment)
    converter = MongoDBCrateDBConverter(transformation=transformation)
    assert converter.decode_document(data_in) == data_out


def test_convert_with_treatment_all_options():
    """
    Validate all treatment options.
    """
    data_in = {
        "_id": {
            "$oid": "56027fcae4b09385a85f9344",
        },
        "ignore_toplevel": 42,
        "value": {
            "id": 42,
            "date": {"$date": "2015-09-23T10:32:42.33Z"},
            "ignore_nested": 42,
        },
        "to_list": 42,
        "to_string": 42,
        "to_dict_scalar": 42,
        "to_dict_list": [{"user": 42}],
    }
    data_out = {
        "_id": "56027fcae4b09385a85f9344",
        "value": {
            "date": 1443004362000,
            "id": 42,
        },
        "to_list": [42],
        "to_string": "42",
        "to_dict_scalar": {"id": 42},
        "to_dict_list": [{"user": {"id": 42}}],
    }

    treatment = Treatment(
        ignore_complex_lists=False,
        ignore_field=["ignore_toplevel", "ignore_nested"],
        convert_list=["to_list"],
        convert_string=["to_string"],
        convert_dict=[
            {"name": "to_dict_scalar", "wrapper_name": "id"},
            {"name": "user", "wrapper_name": "id"},
        ],
    )
    transformation = CollectionTransformation(treatment=treatment)
    converter = MongoDBCrateDBConverter(transformation=transformation)
    assert converter.decode_document(data_in) == data_out


def test_convert_transform_timestamp():
    """
    Validate a Zyp transformation that converts datetime values in text format.
    """
    data_in = [{"device": "Hotzenplotz", "temperature": 42.42, "timestamp": "06/14/2022 12:42:24"}]
    data_out = [{"device": "Hotzenplotz", "temperature": 42.42, "timestamp": 1655203344}]

    bucket_transformation = BucketTransformation(
        values=ValueConverter().add(pointer="/timestamp", transformer="to_unixtime"),
    )
    transformation = CollectionTransformation(bucket=bucket_transformation)
    converter = MongoDBCrateDBConverter(transformation=transformation)
    data = converter.decode_documents(data_in)
    assert data == data_out
