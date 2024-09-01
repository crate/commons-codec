from commons_codec.model import SQLOperation
from commons_codec.transform.dynamodb import DynamoDBFullLoadTranslator

RECORD_ALL_TYPES = {
    "id": {"S": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266"},
    "data": {"M": {"temperature": {"N": "42.42"}, "humidity": {"N": "84.84"}}},
    "meta": {"M": {"timestamp": {"S": "2024-07-12T01:17:42"}, "device": {"S": "foo"}}},
    "location": {
        "M": {
            "coordinates": {"L": [{"S": ""}]},
            "meetingPoint": {"S": "At the end of the tunnel"},
            "address": {"S": "Berchtesgaden Salt Mine"},
        },
    },
    "list_of_objects": {
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
    },
    "list_of_varied": {"L": [{"M": {"a": {"N": 1}}}, {"N": 2}, {"S": "Three"}]},
    "map_of_numbers": {
        "M": {
            "test": {"N": 1},
            "test2": {"N": 2},
        }
    },
    "set_of_binaries": {"BS": ["U3Vubnk="]},
    "set_of_numbers": {"NS": [1, 2, 3, 0.34]},
    "set_of_strings": {"SS": ["location_1"]},
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
                "location": {
                    "address": "Berchtesgaden Salt Mine",
                    "coordinates": [
                        "",
                    ],
                    "meetingPoint": "At the end of the tunnel",
                },
                "list_of_objects": [
                    {
                        "date": "2024-08-28T20:05:42.603Z",
                        "utm_adgroup": ["", ""],
                        "utm_campaign": "34374686341",
                        "utm_medium": "foobar",
                        "utm_source": "google",
                    }
                ],
                "map_of_numbers": {"test": 1.0, "test2": 2.0},
                "set_of_binaries": ["U3Vubnk="],
                "set_of_numbers": [0.34, 1.0, 2.0, 3.0],
                "set_of_strings": ["location_1"],
            },
            "untyped": {
                "list_of_varied": [
                    {"a": 1.0},
                    2.0,
                    "Three",
                ],
            },
        },
    )
