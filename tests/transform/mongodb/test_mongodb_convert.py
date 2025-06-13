# ruff: noqa: E402
import datetime as dt
import typing as t

import pytest

pytest.importorskip("tikray")

from attrs import define

pytestmark = pytest.mark.mongodb

from tikray.model.bucket import BucketTransformation, ValueConverter
from tikray.model.collection import CollectionTransformation
from tikray.model.treatment import Treatment

from commons_codec.transform.mongodb import MongoDBCrateDBConverter

convert_epoch = MongoDBCrateDBConverter.convert_epoch
convert_iso8601 = MongoDBCrateDBConverter.convert_iso8601


def test_epoch_ms_converter_int():
    """
    Datetime values encoded as integer values will be returned unmodified.
    """
    assert convert_epoch(1443004362) == 1443004362
    assert convert_epoch(1443004362987) == 1443004362987


def test_epoch_ms_converter_iso8601():
    """
    Datetime values encoded as ISO8601 values will be parsed.
    """
    assert convert_epoch("2015-09-23T10:32:42.33Z") == 1443004362
    assert convert_epoch(b"2015-09-23T10:32:42.33Z") == 1443004362


def test_epoch_ms_converter_invalid():
    """
    Incorrect datetime values will not be parsed.
    """
    with pytest.raises(ValueError) as ex:
        convert_epoch(None)
    assert ex.match("Unable to convert datetime value: None")


def test_iso8601_converter_int():
    """
    Datetime values encoded as integer values will be returned unmodified.
    """
    assert convert_iso8601(1443004362) == "2015-09-23T10:32:42+00:00"


def test_iso8601_converter_iso8601():
    """
    Datetime values encoded as ISO8601 values will be parsed.
    """
    assert convert_iso8601("2015-09-23T10:32:42.33Z") == "2015-09-23T10:32:42.33Z"
    assert convert_iso8601(b"2015-09-23T10:32:42.33Z") == "2015-09-23T10:32:42.33Z"


def test_iso8601_converter_invalid():
    """
    Incorrect datetime values will not be parsed.
    """
    with pytest.raises(ValueError) as ex:
        convert_iso8601(None)
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


@define
class DateConversionCase:
    converter: t.Callable
    data_in: t.Any
    data_out: t.Any


testdata = [
    DateConversionCase(
        converter=MongoDBCrateDBConverter(),
        data_in={"$date": "2015-09-23T10:32:42.123456Z"},
        data_out=dt.datetime(2015, 9, 23, 10, 32, 42, 123456),
    ),
    DateConversionCase(
        converter=MongoDBCrateDBConverter(),
        data_in={"$date": {"$numberLong": "1655210544987"}},
        data_out=dt.datetime(2022, 6, 14, 12, 42, 24, 987000),
    ),
    DateConversionCase(
        converter=MongoDBCrateDBConverter(timestamp_to_epoch=True, timestamp_use_milliseconds=True),
        data_in={"$date": "2015-09-23T10:32:42.123456Z"},
        data_out=1443004362000,
    ),
    DateConversionCase(
        converter=MongoDBCrateDBConverter(timestamp_to_epoch=True, timestamp_use_milliseconds=True),
        data_in={"$date": {"$numberLong": "1655210544987"}},
        data_out=1655210544000,
    ),
    DateConversionCase(
        converter=MongoDBCrateDBConverter(timestamp_to_iso8601=True),
        data_in={"$date": "2015-09-23T10:32:42.123456Z"},
        data_out="2015-09-23T10:32:42.123456",
    ),
    DateConversionCase(
        converter=MongoDBCrateDBConverter(timestamp_to_iso8601=True),
        data_in={"$date": {"$numberLong": "1655210544987"}},
        data_out="2022-06-14T12:42:24.987000",
    ),
    DateConversionCase(
        converter=MongoDBCrateDBConverter(timestamp_to_iso8601=True),
        data_in={"$date": 1180690093000},
        data_out="2007-06-01T09:28:13",
    ),
]


testdata_ids = [
    "vanilla-$date-canonical",
    "vanilla-$date-legacy",
    "epochms-$date-canonical",
    "epochms-$date-legacy",
    "iso8601-$date-canonical",
    "iso8601-$date-legacy",
    "iso8601-$date-ultra-legacy",
]


@pytest.mark.parametrize("testcase", testdata, ids=testdata_ids)
def test_convert_timestamp_many(testcase: DateConversionCase):
    """
    Verify converting timestamps using different modifiers.
    """
    assert testcase.converter.decode_document(testcase.data_in) == testcase.data_out


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
    converter = MongoDBCrateDBConverter(
        timestamp_to_epoch=True,
        timestamp_use_milliseconds=True,
        transformation=transformation,
    )
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
    converter = MongoDBCrateDBConverter(
        timestamp_to_epoch=True,
        timestamp_use_milliseconds=True,
        transformation=transformation,
    )
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
    converter = MongoDBCrateDBConverter(
        timestamp_to_epoch=True,
        timestamp_use_milliseconds=True,
        transformation=transformation,
    )
    assert converter.decode_document(data_in) == data_out


def test_convert_transform_timestamp():
    """
    Validate a Tikray transformation that converts datetime values in text format.
    """
    data_in = [{"device": "Hotzenplotz", "temperature": 42.42, "timestamp": "06/14/2022 12:42:24.4242 UTC"}]
    data_out = [{"device": "Hotzenplotz", "temperature": 42.42, "timestamp": 1655210544.4242}]

    bucket_transformation = BucketTransformation(
        values=ValueConverter().add(pointer="/timestamp", transformer="to_unixtime"),
    )
    transformation = CollectionTransformation(bucket=bucket_transformation)
    converter = MongoDBCrateDBConverter(
        timestamp_to_epoch=True,
        timestamp_use_milliseconds=True,
        transformation=transformation,
    )
    data = converter.decode_documents(data_in)
    assert data == data_out
