"""
A few samples of MongoDB BSON / JSON structures.

Derived from:
- https://github.com/mongodb/mongo-java-driver/tree/master/bson/src/test/resources/bson
- https://github.com/mongodb/bson-ruby/tree/v5.0.1/spec/spec_tests/data/corpus
"""
# ruff: noqa: ERA001

import datetime as dt
from unittest import mock

import bson

RECORD_IN_ALL_TYPES = {
    "_id": {
        "$oid": "56027fcae4b09385a85f9344",
    },
    "python": {
        "boolean": True,
        "datetime": dt.datetime(2024, 7, 16, 14, 29, 22, 907000),
        "dict_basic": {"foo": "bar"},
        "dict_dollarkey": {"$a": "foo"},
        # "dict_dottedkey": {"a.b": "foo"},  # TODO: InvalidColumnNameException["." contains a dot]
        "dict_empty": {},
        "dict_emptykey": {"": "foo"},
        # "dict_specialkey": {".": "foo", "$": "foo"},  # TODO: InvalidColumnNameException["." contains a dot]
        "float": 42.42,
        "int": 42,
        "list_boolean": [True, False],
        "list_dict": [{"foo": "bar"}],
        "list_empty": [],
        "list_float": [1.1, 2.2, 3.3],
        "list_int": [1, 2, 3],
        "list_string": ["foo", "bar"],
        "null": None,
        "set_int": {1, 2, 3},
        "set_str": {"Räuber", "Hotzenplotz"},
        "str": "Hotzenplotz",
        "tuple_int": (1, 2, 3),
        "tuple_str": ("Räuber", "Hotzenplotz"),
    },
    "bson": {
        "code": bson.code.Code("console.write(foo)", scope={"foo": "bar"}),
        "binary_uuid": bson.Binary(data=b"sccsccsccsccsccs", subtype=bson.binary.UUID_SUBTYPE),
        "datetimems": bson.DatetimeMS(1721140162987),
        "decimal128": bson.Decimal128("42.42"),
        "dbref": bson.DBRef(collection="foo", id="bar", database="baz"),
        "int64": bson.Int64(42.42),
        "maxkey": bson.MaxKey(),
        "minkey": bson.MinKey(),
        "objectid": bson.ObjectId("669683c2b0750b2c84893f3e"),
        "regex": bson.regex.Regex(".*"),
        "timestamp": bson.Timestamp(1721140162, 2),
    },
    "canonical": {
        "code_ascii": {"$code": "abab"},
        "code_bytes": {"$code": "ab\u0000ab\u0000"},
        "code_scope": {"$code": "abab", "$scope": {"x": {"$numberInt": "42"}}},
        "date_iso8601": {"$date": "2015-09-23T10:32:42.33Z"},
        "date_numberlong_valid": {"$date": {"$numberLong": "1356351330000"}},
        "date_numberlong_invalid": {
            "$date": {"$numberLong": "-9223372036854775808"}
        },  # year -292275055 is out of range
        "dbref": {
            "$id": {"$oid": "56027fcae4b09385a85f9344"},
            "$ref": "foo",
            "$db": "bar",
        },
        "decimal_infinity": {"$numberDecimal": "Infinity"},
        "decimal_largest": {"$numberDecimal": "1234567890123456789012345678901234"},
        "decimal_nan": {"$numberDecimal": "NaN"},
        "decimal_regular": {"$numberDecimal": "0.000001234567890123456789012345678901234"},
        # "double_infinity": {"$numberDouble": "Infinity"},  # TODO: SQLParseException[Failed to parse source
        "double_regular": {"$numberDouble": "-1.2345678921232E+18"},
        "int32": {"$numberInt": "-2147483648"},
        "int64": {"$numberLong": "-9223372036854775808"},
        "list_date": [
            {"$date": "2015-09-24T10:32:42.33Z"},
            {"$date": {"$numberLong": "2147483647000"}},
            {"$date": {"$numberLong": "-2147483648000"}},
        ],
        "list_dict": [
            {"id": "bar", "value": {"$date": "2015-09-24T10:32:42.33Z"}},
            {"value": {"$date": "2015-09-24T10:32:42.33Z"}},
        ],
        "list_int": [
            {"$numberInt": "-2147483648"},
        ],
        "list_oid": [
            {"$oid": "56027fcae4b09385a85f9344"},
        ],
        "list_uuid": [
            # TODO: TypeError: Object of type bytes is not JSON serializable
            # {"$binary": {"base64": "c//SZESzTGmQ6OfR38A11A==", "subType": "00"}},
            {"$binary": {"base64": "c//SZESzTGmQ6OfR38A11A==", "subType": "01"}},
            {"$binary": {"base64": "c//SZESzTGmQ6OfR38A11A==", "subType": "02"}},
            {"$binary": {"base64": "c//SZESzTGmQ6OfR38A11A==", "subType": "03"}},
            {"$binary": {"base64": "c//AYDC420csII3929483B==", "subType": "04"}},
            {"$binary": {"base64": "c//AYDC420csII3929483B==", "subType": "05"}},
            {"$binary": {"base64": "c//AYDC420csII3929483B==", "subType": "06"}},
            {"$binary": {"base64": "c//AYDC420csII3929483B==", "subType": "80"}},
        ],
        "maxkey": {"$maxKey": 1},
        "minkey": {"$minKey": 1},
        "oid": {"$oid": "56027fcae4b09385a85f9344"},
        "regex": {"$regularExpression": {"pattern": ".*", "options": ""}},
        "symbol": {"$symbol": "foo"},
        "timestamp": {"$timestamp": {"t": 123456789, "i": 42}},
        # TODO: Implement other UUID subtypes.
        #       https://github.com/mongodb/bson-ruby/blob/v5.0.1/spec/spec_tests/data/corpus/binary.json
        "undefined": {"$undefined": True},
        "uuid": {"$binary": {"base64": "c//SZESzTGmQ6OfR38A11A==", "subType": "04"}},
    },
}

RECORD_OUT_ALL_TYPES = {
    "_id": "56027fcae4b09385a85f9344",
    "python": {
        "boolean": True,
        "datetime": 1721140162000,
        "dict_basic": {"foo": "bar"},
        "dict_dollarkey": {"$a": "foo"},
        # "dict_dottedkey": {'a.b': 'foo'},  # TODO: InvalidColumnNameException["." contains a dot]
        "dict_empty": {},
        "dict_emptykey": {"": "foo"},
        # "dict_specialkey": {'$': 'foo', '.': 'foo'},  # TODO: InvalidColumnNameException["." contains a dot]
        "float": 42.42,
        "int": 42,
        "list_boolean": [True, False],
        "list_dict": [{"foo": "bar"}],
        "list_empty": [],
        "list_float": [1.1, 2.2, 3.3],
        "list_int": [1, 2, 3],
        "list_string": ["foo", "bar"],
        "null": None,
        "set_int": [1, 2, 3],
        "set_str": mock.ANY,
        "str": "Hotzenplotz",
        "tuple_int": [1, 2, 3],
        "tuple_str": ["Räuber", "Hotzenplotz"],
    },
    "bson": {
        "code": {
            "$code": "console.write(foo)",
            "$scope": {
                "foo": "bar",
            },
        },
        "datetimems": 1721140162000,
        "binary_uuid": "73636373-6363-7363-6373-636373636373",
        "decimal128": "42.42",
        "dbref": {
            "$ref": "foo",
            "$id": "bar",
            "$db": "baz",
        },
        "int64": 42,
        "maxkey": "MaxKey()",
        "minkey": "MinKey()",
        "objectid": "669683c2b0750b2c84893f3e",
        "regex": "Regex('.*', 0)",
        "timestamp": 1721140162000,
    },
    "canonical": {
        "code_ascii": "abab",
        "code_bytes": "ab\x00ab\x00",
        "code_scope": {
            "$code": "abab",
            "$scope": {
                "x": 42,
            },
        },
        "date_iso8601": 1443004362000,
        "date_numberlong_valid": 1356351330000,
        "date_numberlong_invalid": 0,
        "dbref": {
            "$id": "56027fcae4b09385a85f9344",
            "$ref": "foo",
            "$db": "bar",
        },
        "decimal_infinity": "Infinity",
        "decimal_largest": "1234567890123456789012345678901234",
        "decimal_nan": "NaN",
        "decimal_regular": "0.000001234567890123456789012345678901234",
        "double_regular": -1.2345678921232e18,
        "int32": -2147483648,
        "int64": "-9223372036854775808",  # TODO: Representation as string is just fine?
        "list_date": [
            1443090762000,
            2147483647000,
            -2147483648000,
        ],
        "list_dict": [
            {"id": "bar", "value": 1443090762000},
            {"value": 1443090762000},
        ],
        "list_int": [
            -2147483648,
        ],
        "list_oid": [
            "56027fcae4b09385a85f9344",
        ],
        "list_uuid": [
            # TODO: TypeError: Object of type bytes is not JSON serializable
            # b's\xff\xd2dD\xb3Li\x90\xe8\xe7\xd1\xdf\xc05\xd4',
            "c//SZESzTGmQ6OfR38A11A==",
            "c//SZESzTGmQ6OfR38A11A==",
            "c//SZESzTGmQ6OfR38A11A==",
            "73ffc060-30b8-db47-2c20-8dfddbde3cdc",
            "c//AYDC420csII3929483A==",
            "c//AYDC420csII3929483A==",
            "c//AYDC420csII3929483A==",
        ],
        "maxkey": "MaxKey()",
        "minkey": "MinKey()",
        "oid": "56027fcae4b09385a85f9344",
        "regex": "Regex('.*', 0)",
        "symbol": "foo",
        "timestamp": 123456789000,
        "undefined": None,
        "uuid": "73ffd264-44b3-4c69-90e8-e7d1dfc035d4",
    },
}


RECORD_IN_ANOMALIES = {
    "_id": {
        "$oid": "56027fcae4b09385a85f9344",
    },
    "python": {
        "list_of_nested_list": [1, [2, 3], 4],
        "list_of_objects": [{}],
        "to_dict": 123,
        "to_list": 123,
        "to_string": 123,
    },
}

RECORD_OUT_ANOMALIES = {
    "_id": "56027fcae4b09385a85f9344",
    "python": {
        "list_of_nested_list": [1, 2, 3, 4],
        "list_of_objects": None,
        "to_dict": {"id": 123},
        "to_list": [123],
        "to_string": "123",
    },
}
