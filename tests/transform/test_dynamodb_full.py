from commons_codec.model import SQLOperation
from commons_codec.transform.dynamodb import DynamoDBFullLoadTranslator

RECORD_ALL_TYPES = {
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
    "list_varied": {"L": [{"M": {"a": {"N": 1}}}, {"N": 2}, {"S": "Three"}]},
}

RECORD_UTM = {
    "utmTags": {
        "L": [
            {
                "M": {
                    "date": {"S": "2024-08-28T20:05:42.603Z"},
                    "utm_adgroup": {"L": [{"S": ""}, {"S": ""}]},
                    "utm_campaign": {"S": "34374686341"},
                    "utm_medium": {"S": "foobar"},
                    "utm_source": {"S": "google"},
                }
            }
        ]
    }
}


def test_sql_ddl():
    assert (
        DynamoDBFullLoadTranslator(table_name="foo").sql_ddl
        == "CREATE TABLE IF NOT EXISTS foo (data OBJECT(DYNAMIC), aux OBJECT(IGNORED));"
    )


def test_to_sql_all_types():
    assert DynamoDBFullLoadTranslator(table_name="foo").to_sql(RECORD_ALL_TYPES) == SQLOperation(
        statement="INSERT INTO foo (data, aux) VALUES (:typed, :untyped);",
        parameters={
            "typed": {
                "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
                "data": {"temperature": 42.42, "humidity": 84.84},
                "meta": {"timestamp": "2024-07-12T01:17:42", "device": "foo"},
                "string_set": ["location_1"],
                "number_set": [0.34, 1.0, 2.0, 3.0],
                "binary_set": ["U3Vubnk="],
                "somemap": {"test": 1.0, "test2": 2.0},
            },
            "untyped": {
                "list_varied": [
                    {"a": 1.0},
                    2.0,
                    "Three",
                ],
            },
        },
    )


def test_to_sql_list_of_objects():
    assert DynamoDBFullLoadTranslator(table_name="foo").to_sql(RECORD_UTM) == SQLOperation(
        statement="INSERT INTO foo (data, aux) VALUES (:typed, :untyped);",
        parameters={
            "typed": {
                "utmTags": [
                    {
                        "date": "2024-08-28T20:05:42.603Z",
                        "utm_adgroup": ["", ""],
                        "utm_campaign": "34374686341",
                        "utm_medium": "foobar",
                        "utm_source": "google",
                    }
                ]
            },
            "untyped": {},
        },
    )
