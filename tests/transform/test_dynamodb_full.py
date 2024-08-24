from commons_codec.model import SQLOperation
from commons_codec.transform.dynamodb import DynamoDBFullLoadTranslator

RECORD = {
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
}


def test_sql_ddl():
    assert (
        DynamoDBFullLoadTranslator(table_name="foo").sql_ddl
        == 'CREATE TABLE IF NOT EXISTS "foo" (data OBJECT(DYNAMIC));'
    )


def test_to_sql():
    assert DynamoDBFullLoadTranslator(table_name="foo").to_sql(RECORD) == SQLOperation(
        statement='INSERT INTO "foo" (data) VALUES (:record);',
        parameters={
            "record": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
                "data": {"temperature": 42.42, "humidity": 84.84},
                "meta": {"timestamp": "2024-07-12T01:17:42", "device": "foo"},
                "string_set": ["location_1"],
                "number_set": [0.34, 1.0, 2.0, 3.0],
                "binary_set": ["U3Vubnk="],
                "somemap": {"test": 1.0, "test2": 2.0},
            }
        },
    )
