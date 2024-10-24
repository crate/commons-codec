from collections import Counter
from decimal import Decimal

import pytest

from commons_codec.model import SQLOperation, UniversalRecord
from commons_codec.transform.dynamodb import CrateDBTypeDeserializer, DynamoDBCDCTranslator, DynamoDBFullLoadTranslator

pytestmark = pytest.mark.dynamodb

READING_BASIC = {"device": "foo", "temperature": 42.42, "humidity": 84.84}

MSG_UNKNOWN_SOURCE = {
    "eventSource": "foo:bar",
}
MSG_UNKNOWN_EVENT = {
    "eventSource": "aws:dynamodb",
    "eventName": "FOOBAR",
}

MSG_INSERT_BASIC = {
    "awsRegion": "us-east-1",
    "eventID": "b015b5f0-c095-4b50-8ad0-4279aa3d88c6",
    "eventName": "INSERT",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "foo",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720740233012995,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "NewImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "humidity": {"N": "84.84"},
            "temperature": {"N": "42.42"},
            "device": {"S": "foo"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 4]},
            "binary_set": {"BS": ["U3Vubnk="]},
        },
        "SizeBytes": 99,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}
MSG_INSERT_NESTED = {
    "awsRegion": "us-east-1",
    "eventID": "b581c2dc-9d97-44ed-94f7-cb77e4fdb740",
    "eventName": "INSERT",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "table-testdrive-nested",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720800199717446,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "NewImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "data": {"M": {"temperature": {"N": "42.42"}, "humidity": {"N": "84.84"}}},
            "meta": {"M": {"timestamp": {"S": "2024-07-12T01:17:42"}, "device": {"S": "foo"}}},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 0.34]},
            "binary_set": {"BS": ["U3Vubnk="]},
            "somemap": {
                "M": {
                    "test": {"N": 1},
                    "test2": {"N": 2},
                }
            },
        },
        "SizeBytes": 156,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}
MSG_MODIFY_BASIC = {
    "awsRegion": "us-east-1",
    "eventID": "24757579-ebfd-480a-956d-a1287d2ef707",
    "eventName": "MODIFY",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "foo",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720742302233719,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "NewImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "humidity": {"N": "84.84"},
            "temperature": {"N": "55.66"},
            "device": {"S": "bar"},
            "location": {"S": "Sydney"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 0.34]},
            "binary_set": {"BS": ["U3Vubnk="]},
            "empty_string": {"S": ""},
            "null_string": {"S": None},
        },
        "OldImage": {
            "humidity": {"N": "84.84"},
            "temperature": {"N": "42.42"},
            "device": {"S": "foo"},
            "location": {"S": "Sydney"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
        },
        "SizeBytes": 161,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}
MSG_MODIFY_NESTED = {
    "awsRegion": "us-east-1",
    "eventID": "24757579-ebfd-480a-956d-a1287d2ef707",
    "eventName": "MODIFY",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "foo",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720742302233719,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "NewImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "device": {"S": "foo"},
            "tags": {"L": [{"S": "foo"}, {"S": "bar"}]},
            "empty_map": {"M": {}},
            "empty_list": {"L": []},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 0.34]},
            "binary_set": {"BS": ["U3Vubnk="]},
            "somemap": {
                "M": {
                    "test": {"N": 1},
                    "test2": {"N": 2},
                }
            },
            "list_of_objects": {"L": [{"M": {"foo": {"S": "bar"}}}, {"M": {"baz": {"S": "qux"}}}]},
        },
        "OldImage": {
            "humidity": {"N": "84.84"},
            "temperature": {"N": "42.42"},
            "location": {"S": "Sydney"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "device": {"M": {"id": {"S": "bar"}, "serial": {"N": 12345}}},
        },
        "SizeBytes": 161,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}
MSG_REMOVE = {
    "awsRegion": "us-east-1",
    "eventID": "ff4e68ab-0820-4a0c-80b2-38753e8e00e5",
    "eventName": "REMOVE",
    "userIdentity": None,
    "recordFormat": "application/json",
    "tableName": "foo",
    "dynamodb": {
        "ApproximateCreationDateTime": 1720742321848352,
        "Keys": {"id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"}},
        "OldImage": {
            "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
            "humidity": {"N": "84.84"},
            "temperature": {"N": "55.66"},
            "device": {"S": "bar"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
            "string_set": {"SS": ["location_1"]},
            "number_set": {"NS": [1, 2, 3, 0.34]},
            "binary_set": {"BS": ["U3Vubnk="]},
            "somemap": {
                "M": {
                    "test": {"N": 1},
                    "test2": {"N": 2},
                }
            },
        },
        "SizeBytes": 99,
        "ApproximateCreationDateTimePrecision": "MICROSECOND",
    },
    "eventSource": "aws:dynamodb",
}


def test_decode_ddb_deserialize_type(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.decode_record({"foo": {"N": "84.84"}}) == UniversalRecord(
        pk={}, typed={"foo": 84.84}, untyped={}
    )


def test_decode_cdc_unknown_source(dynamodb_cdc_translator_foo):
    with pytest.raises(ValueError) as ex:
        dynamodb_cdc_translator_foo.to_sql(MSG_UNKNOWN_SOURCE)
    assert ex.match("Unknown eventSource: foo:bar")


def test_decode_cdc_unknown_event(dynamodb_cdc_translator_foo):
    with pytest.raises(ValueError) as ex:
        dynamodb_cdc_translator_foo.to_sql(MSG_UNKNOWN_EVENT)
    assert ex.match("Unknown CDC event name: FOOBAR")


def test_decode_cdc_insert_basic(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.to_sql(MSG_INSERT_BASIC) == SQLOperation(
        statement="INSERT INTO foo (pk, data, aux) VALUES (:pk, :typed, :untyped) ON CONFLICT DO NOTHING;",
        parameters={
            "pk": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
            },
            "typed": {
                "humidity": 84.84,
                "temperature": 42.42,
                "device": "foo",
                "timestamp": "2024-07-12T01:17:42",
                "string_set": ["location_1"],
                "number_set": [1.0, 2.0, 3.0, 4.0],
                "binary_set": ["U3Vubnk="],
            },
            "untyped": {},
        },
    )


def test_decode_cdc_insert_nested(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.to_sql(MSG_INSERT_NESTED) == SQLOperation(
        statement="INSERT INTO foo (pk, data, aux) VALUES (:pk, :typed, :untyped) ON CONFLICT DO NOTHING;",
        parameters={
            "pk": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
            },
            "typed": {
                "data": {"temperature": 42.42, "humidity": 84.84},
                "meta": {"timestamp": "2024-07-12T01:17:42", "device": "foo"},
                "string_set": ["location_1"],
                "number_set": [0.34, 1.0, 2.0, 3.0],
                "binary_set": ["U3Vubnk="],
                "somemap": {"test": 1.0, "test2": 2.0},
            },
            "untyped": {},
        },
    )


def test_decode_cdc_modify_basic(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.to_sql(MSG_MODIFY_BASIC) == SQLOperation(
        statement="UPDATE foo SET data=:typed, aux=:untyped WHERE pk=:pk;",
        parameters={
            "pk": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
            },
            "typed": {
                "device": "bar",
                "timestamp": "2024-07-12T01:17:42",
                "humidity": 84.84,
                "temperature": 55.66,
                "location": "Sydney",
                "string_set": ["location_1"],
                "number_set": [0.34, 1.0, 2.0, 3.0],
                "binary_set": ["U3Vubnk="],
                "empty_string": "",
                "null_string": None,
            },
            "untyped": {},
        },
    )


def test_decode_cdc_modify_nested(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.to_sql(MSG_MODIFY_NESTED) == SQLOperation(
        statement="UPDATE foo SET data=:typed, aux=:untyped WHERE pk=:pk;",
        parameters={
            "pk": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
            },
            "typed": {
                "device": "foo",
                "timestamp": "2024-07-12T01:17:42",
                "tags": ["foo", "bar"],
                "empty_map": {},
                "empty_list": [],
                "string_set": ["location_1"],
                "number_set": [0.34, 1.0, 2.0, 3.0],
                "binary_set": ["U3Vubnk="],
                "somemap": {"test": 1.0, "test2": 2.0},
                "list_of_objects": [{"foo": "bar"}, {"baz": "qux"}],
            },
            "untyped": {},
        },
    )


def test_decode_cdc_remove(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.to_sql(MSG_REMOVE) == SQLOperation(
        statement="DELETE FROM foo WHERE pk=:pk;",
        parameters={
            "pk": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
            },
            "typed": {},
            "untyped": {},
        },
    )


def test_deserialize_number_set():
    deserializer = CrateDBTypeDeserializer()
    assert deserializer.deserialize({"NS": ["1", "1.25"]}) == [
        Decimal("1"),
        Decimal("1.25"),
    ]


def test_deserialize_string_set():
    deserializer = CrateDBTypeDeserializer()
    # We us Counter because when the set is transformed into a list, it loses order.
    assert Counter(deserializer.deserialize({"SS": ["foo", "bar"]})) == Counter(
        [
            "foo",
            "bar",
        ]
    )


def test_deserialize_binary_set():
    deserializer = CrateDBTypeDeserializer()
    assert Counter(deserializer.deserialize({"BS": [b"\x00", b"\x01"]})) == Counter([b"\x00", b"\x01"])


def test_deserialize_list_objects():
    deserializer = CrateDBTypeDeserializer()
    assert deserializer.deserialize(
        {
            "L": [
                {
                    "M": {
                        "foo": {"S": "mystring"},
                        "bar": {"M": {"baz": {"N": "1"}}},
                    }
                },
                {
                    "M": {
                        "foo": {"S": "other string"},
                        "bar": {"M": {"baz": {"N": "2"}}},
                    }
                },
            ]
        }
    )


@pytest.mark.integration
def test_to_sql_cratedb(caplog, cratedb, dynamodb_full_translator_foo):
    """
    Verify converging NewImage CDC INSERT events to CrateDB.
    """

    # Use the full-load translator only for running the SQL DDL.
    translator = DynamoDBFullLoadTranslator(
        table_name="from.dynamodb", primary_key_schema=dynamodb_full_translator_foo.primary_key_schema
    )
    cratedb.database.run_sql(translator.sql_ddl)

    # Compute CrateDB operation (SQL+parameters) from DynamoDB record.
    translator = DynamoDBCDCTranslator(table_name="from.dynamodb")
    operation = translator.to_sql(MSG_INSERT_NESTED)

    # Insert into CrateDB. Running the INSERT operation twice proves it
    # does not raise any `DuplicateKeyException`, because CDC INSERT
    # statements use `ON CONFLICT DO NOTHING`.
    cratedb.database.run_sql(operation.statement, operation.parameters)
    cratedb.database.run_sql(operation.statement, operation.parameters)

    # Verify data in target database.
    assert cratedb.database.table_exists("from.dynamodb") is True
    assert cratedb.database.refresh_table("from.dynamodb") is True
    assert cratedb.database.count_records("from.dynamodb") == 1

    results = cratedb.database.run_sql('SELECT * FROM "from".dynamodb;', records=True)  # noqa: S608
    assert results[0]["pk"]["id"] == MSG_INSERT_NESTED["dynamodb"]["NewImage"]["id"]["S"]
    assert results[0]["data"]["meta"]["timestamp"] == "2024-07-12T01:17:42"
    assert results[0]["aux"] == {}
