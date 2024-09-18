"""
A few samples of MongoDB BSON / JSON structures.

Derived from:
https://github.com/mongodb/bson-ruby/tree/v5.0.1/spec/spec_tests/data/corpus
"""

import datetime as dt
from unittest import mock

import bson

RECORD_IN_ALL_TYPES = {
    "_id": {
        "$oid": "56027fcae4b09385a85f9344",
    },
    "python": {
        "bool": True,
        "datetime": dt.datetime(2024, 7, 16, 14, 29, 22, 907000),
        "dict_basic": {"foo": "bar"},
        "float": 42.42,
        "int": 42,
        "list_bool": [True, False],
        "list_dict": [{"foo": "bar"}],
        "list_float": [1.1, 2.2, 3.3],
        "list_int": [1, 2, 3],
        "list_string": ["foo", "bar"],
        "set_int": {1, 2, 3},
        "set_str": {"Räuber", "Hotzenplotz"},
        "str": "Hotzenplotz",
        "tuple_int": (1, 2, 3),
        "tuple_str": ("Räuber", "Hotzenplotz"),
    },
    "bson": {
        "decimal128": bson.Decimal128("42.42"),
        "dbref": bson.DBRef(id="foo", collection="bar", database="baz"),
        "int64": bson.Int64(42.42),
        "objectid": bson.ObjectId("669683c2b0750b2c84893f3e"),
        "timestamp": bson.Timestamp(1721140162, 2),
    },
    "canonical": {
        "date_iso8601": {"$date": "2015-09-23T10:32:42.33Z"},
        "date_numberlong": {"$date": {"$numberLong": "1356351330501"}},
        "double": {"$numberDouble": "-1.2345678921232E+18"},
        "int32": {"$numberInt": "-2147483648"},
        "int64": {"$numberLong": "-9223372036854775808"},
        "list_dict": [
            {"id": "bar", "value": {"$date": "2015-09-24T10:32:42.33Z"}},
        ],
        "list_int": [
            {"$numberInt": "-2147483648"},
        ],
        "oid": {"$oid": "56027fcae4b09385a85f9344"},
        "uuid": {"$binary": {"base64": "c//SZESzTGmQ6OfR38A11A==", "subType": "04"}},
    },
}

RECORD_OUT_ALL_TYPES = {
    "_id": "56027fcae4b09385a85f9344",
    "python": {
        "bool": True,
        "datetime": 1721140162000,
        "dict_basic": {"foo": "bar"},
        "float": 42.42,
        "int": 42,
        "list_bool": [True, False],
        "list_dict": [{"foo": "bar"}],
        "list_float": [1.1, 2.2, 3.3],
        "list_int": [1, 2, 3],
        "list_string": ["foo", "bar"],
        "set_int": [1, 2, 3],
        "set_str": mock.ANY,
        "str": "Hotzenplotz",
        "tuple_int": [1, 2, 3],
        "tuple_str": ["Räuber", "Hotzenplotz"],
    },
    "bson": {
        "decimal128": "42.42",
        "dbref": {
            "$id": "foo",
            "$ref": "bar",
            "$db": "baz",
        },
        "int64": 42,
        "objectid": "669683c2b0750b2c84893f3e",
        "timestamp": 1721140162000,
    },
    "canonical": {
        "date_iso8601": 1443004362000,
        "date_numberlong": 1356351330000,
        "double": -1.2345678921232e18,
        "int32": -2147483648,
        "int64": "-9223372036854775808",  # TODO: Encoding as string is just fine?
        "list_dict": [
            {"id": "bar", "value": 1443090762000},
        ],
        "list_int": [
            -2147483648,
        ],
        "oid": "56027fcae4b09385a85f9344",
        "uuid": "73ffd264-44b3-4c69-90e8-e7d1dfc035d4",
    },
}
