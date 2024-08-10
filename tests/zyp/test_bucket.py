# ruff: noqa: E402
import datetime as dt
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest
from zyp.model.base import SchemaDefinition
from zyp.model.bucket import (
    BucketTransformation,
    FieldRenamer,
    TransonTransformation,
    ValueConverter,
)
from zyp.model.collection import CollectionTransformation


class ReadingWithTimestamps:
    """
    An example dataset including a variety of timestamps.

    null
    06/30/2023
    07/31/2022 00:00:00
    2022-07-07
    Invalid date
    """

    ingress = {
        "meta": {
            "american_date": "06/30/2023",
            "american_date_time": "06/14/2022 12:42:24",
            "empty_date": "",
            "international_date": "2022-07-07",
            "invalid_date": "Invalid date",
            "none_date": None,
            "null_date": "null",
        },
        "data": {
            "temperature": 42.42,
            "humidity": 84.84,
        },
    }
    egress = {
        "meta": {
            "american_date": dt.datetime(2023, 6, 30, 0, 0, 0),
            "american_date_time": dt.datetime(2022, 6, 14, 12, 42, 24),
            "empty_date": None,
            "international_date": dt.datetime(2022, 7, 7),
            "invalid_date": None,
            "none_date": None,
            "null_date": None,
        },
        "data": {
            "temperature": 42.42,
            "humidity": 84.84,
        },
    }


class BasicReading:
    ingress = {
        "_id": "foobar",
        "meta": {
            "date": "06/14/2022 12:42:24",
        },
        "data": {
            "temperature": 42.42,
            "humidity": 84.84,
        },
    }
    egress = {
        "id": "foobar",
        "meta": {
            "date": 1655203344.0,
        },
        "data": {
            "temperature": 42.42,
            "humidity": 84.84,
        },
    }


def test_value_converter_datetime_function_reference():
    """
    Verify value conversion with function reference to built-in transformer.
    """
    engine = ValueConverter()
    engine.add(pointer="/meta/american_date", transformer="to_datetime")
    engine.add(pointer="/meta/american_date_time", transformer="to_datetime")
    engine.add(pointer="/meta/empty_date", transformer="to_datetime")
    engine.add(pointer="/meta/international_date", transformer="to_datetime")
    engine.add(pointer="/meta/invalid_date", transformer="to_datetime")
    engine.add(pointer="/meta/none_date", transformer="to_datetime")
    engine.add(pointer="/meta/null_date", transformer="to_datetime")

    indata = deepcopy(ReadingWithTimestamps.ingress)
    outdata = engine.apply(indata)
    assert outdata == ReadingWithTimestamps.egress


def test_value_converter_datetime_function_callback():
    """
    Verify value conversion with function callback.

    Note: This use-case is discouraged, because an inline callback can't
          be serialized into a text representation well.
    """
    engine = ValueConverter()
    from zyp.function import to_datetime

    engine.add(pointer="/meta/american_date", transformer=to_datetime)
    indata = deepcopy(ReadingWithTimestamps.ingress)
    outdata = engine.apply(indata)
    assert outdata["meta"]["american_date"] == ReadingWithTimestamps.egress["meta"]["american_date"]


def test_value_converter_root_node_yaml_dump():
    """
    Converting values on the root level of the document.
    """
    engine = ValueConverter()
    engine.add(pointer="", transformer="yaml.dump")
    assert engine.apply({"value": 42}) == "value: 42\n"


def test_value_converter_root_node_extract_and_convert():
    """
    Converting values on the root level of the document.
    """
    engine = ValueConverter()
    engine.add(pointer="", transformer="operator.itemgetter", args=["value"])
    engine.add(pointer="", transformer="builtins.str")
    assert engine.apply({"value": 42}) == "42"


def test_value_converter_path_invalid():
    """
    Converting values with an invalid location pointer fails.
    """
    engine = ValueConverter()
    with pytest.raises(ValueError) as ex:
        engine.add(pointer="---", transformer="to_datetime")
    assert ex.match("Location must start with /")


def test_value_converter_transformer_empty():
    """
    Converting values with an empty transformer reference fails.
    """
    engine = ValueConverter()
    with pytest.raises(ValueError) as ex:
        engine.add(pointer="/foo", transformer="")
    assert ex.match("Empty transformer reference")


def test_value_converter_transformer_unknown_module():
    """
    Converting values with an unknown transformer module fails.
    """
    engine = ValueConverter()
    with pytest.raises(ImportError) as ex:
        engine.add(pointer="/foo", transformer="foo.to_unknown")
    assert ex.match("No module named 'foo'")


def test_value_converter_transformer_unknown_symbol():
    """
    Converting values with an unknown transformer symbol fails.
    """
    engine = ValueConverter()
    with pytest.raises(AttributeError) as ex:
        engine.add(pointer="/foo", transformer="to_unknown")
    assert ex.match("module 'zyp.function' has no attribute 'to_unknown'")


def test_bucket_transformation_success():
    """
    Converting values with a complete transformation description.
    """
    transformation = BucketTransformation(
        names=FieldRenamer().add(old="_id", new="id"),
        values=ValueConverter().add(pointer="/meta/date", transformer="to_unixtime"),
    )
    result = transformation.apply(deepcopy(BasicReading.ingress))
    assert result == BasicReading.egress


def test_bucket_transformation_transon_compute():
    """
    Converting documents using a `transon` transformation.
    https://transon-org.github.io/
    """
    transformation = BucketTransformation(
        transon=TransonTransformation().add(
            pointer="/abc", template={"$": "call", "name": "str", "value": {"$": "expr", "op": "mul", "value": 2}}
        ),
    )
    result = transformation.apply({"abc": 123})
    assert result == {"abc": "246"}


def test_bucket_transformation_transon_filter():
    """
    Converting documents using a `transon` transformation.
    https://transon-org.github.io/
    """
    transformation = BucketTransformation(
        transon=TransonTransformation().add(
            pointer="", template={"$": "filter", "cond": {"$": "expr", "op": "!=", "values": [{"$": "key"}, "baz"]}}
        ),
    )
    result = transformation.apply({"foo": "bar", "baz": "qux", "123": "456"})
    assert result == {"foo": "bar", "123": "456"}


def test_bucket_transformation_success_2():
    """
    Running a transformation without any manipulations yields the original input value.
    """
    transformation = BucketTransformation()
    result = transformation.apply(deepcopy(BasicReading.ingress))
    assert result == BasicReading.ingress


def test_bucket_transformation_serialize():
    """
    A transformation description can be serialized to a data structure and back.
    """
    transformation = BucketTransformation(
        schema=SchemaDefinition().add(pointer="/meta/date", type="DATETIME"),
        names=FieldRenamer().add(old="_id", new="id"),
        values=ValueConverter().add(pointer="/meta/date", transformer="to_unixtime"),
    )
    transformation_dict = {
        "meta": {"version": 1, "type": "zyp-bucket"},
        "schema": {"rules": [{"pointer": "/meta/date", "type": "DATETIME"}]},
        "names": {"rules": [{"new": "id", "old": "_id"}]},
        "values": {"rules": [{"pointer": "/meta/date", "transformer": "to_unixtime"}]},
    }
    result = transformation.to_dict()
    assert result == transformation_dict

    result = transformation.to_json()
    assert json.loads(result) == transformation_dict


def test_bucket_transformation_serialize_args():
    """
    Check if transformer args are also serialized.
    """
    transformation = BucketTransformation(
        values=ValueConverter().add(pointer="", transformer="operator.itemgetter", args=["value"]),
    )
    result = transformation.to_dict()
    transformation_dict = {
        "meta": {"version": 1, "type": "zyp-bucket"},
        "values": {"rules": [{"pointer": "", "transformer": "operator.itemgetter", "args": ["value"]}]},
    }
    assert result == transformation_dict


def test_bucket_transformation_load_and_apply():
    """
    Verify transformation can be loaded from JSON and applied again.
    """
    payload = Path("tests/zyp/transformation-bucket.json").read_text()
    transformation = BucketTransformation.from_json(payload)
    result = transformation.apply(deepcopy(BasicReading.ingress))
    assert result == BasicReading.egress


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Does not work on Python 3.8 and earlier")
def test_bucket_transon_marshal():
    """
    Verify transformation can be loaded from JSON and applied again.
    """
    transformation = BucketTransformation(
        transon=TransonTransformation().add(
            pointer="/abc", template={"$": "call", "name": "str", "value": {"$": "expr", "op": "mul", "value": 2}}
        ),
    )
    BucketTransformation.from_yaml(transformation.to_yaml())


def test_from_dict():
    assert isinstance(BucketTransformation.from_dict({}), BucketTransformation)
    assert isinstance(CollectionTransformation.from_dict({}), CollectionTransformation)
