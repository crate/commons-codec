import pytest

from commons_codec.model import SQLOperation
from commons_codec.transform.dynamodb import DynamoDBFullLoadTranslator

pytestmark = pytest.mark.dynamodb

RECORD_IN = {
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

RECORD_OUT_PK = {
    "id": "5F9E-Fsadd41C-4C92-A8C1-70BF3FFB9266",
}

RECORD_OUT_DATA = {
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
}

RECORD_OUT_AUX = {
    "list_of_varied": [
        {"a": 1.0},
        2.0,
        "Three",
    ],
}


def test_sql_ddl_success(dynamodb_full_translator_foo):
    assert (
        dynamodb_full_translator_foo.sql_ddl == "CREATE TABLE IF NOT EXISTS foo "
        '(pk OBJECT(STRICT) AS ("id" STRING PRIMARY KEY), data OBJECT(DYNAMIC), aux OBJECT(IGNORED));'
    )


def test_sql_ddl_failure(dynamodb_full_translator_foo):
    translator = DynamoDBFullLoadTranslator(table_name="foo")
    with pytest.raises(IOError) as ex:
        _ = translator.sql_ddl
    assert ex.match("Unable to generate SQL DDL without key schema information")


def test_to_sql_operation(dynamodb_full_translator_foo):
    """
    Verify outcome of `DynamoDBFullLoadTranslator.to_sql` operation.
    """
    assert dynamodb_full_translator_foo.to_sql(RECORD_IN) == SQLOperation(
        statement="INSERT INTO foo (pk, data, aux) VALUES (:pk, :typed, :untyped);",
        parameters=[
            {
                "pk": RECORD_OUT_PK,
                "typed": RECORD_OUT_DATA,
                "untyped": RECORD_OUT_AUX,
            }
        ],
    )


@pytest.mark.integration
def test_to_sql_cratedb(caplog, cratedb, dynamodb_full_translator_foo):
    """
    Verify writing converted DynamoDB record to CrateDB.
    """

    # Compute CrateDB operation (SQL+parameters) from DynamoDB record.
    translator = DynamoDBFullLoadTranslator(
        table_name="from.dynamodb", primary_key_schema=dynamodb_full_translator_foo.primary_key_schema
    )
    operation = translator.to_sql(RECORD_IN)

    # Insert into CrateDB.
    cratedb.database.run_sql(translator.sql_ddl)
    cratedb.database.run_sql(operation.statement, operation.parameters)

    # Verify data in target database.
    assert cratedb.database.table_exists("from.dynamodb") is True
    assert cratedb.database.refresh_table("from.dynamodb") is True
    assert cratedb.database.count_records("from.dynamodb") == 1

    results = cratedb.database.run_sql('SELECT * FROM "from".dynamodb;', records=True)  # noqa: S608
    assert results[0]["pk"] == RECORD_OUT_PK
    assert results[0]["data"] == RECORD_OUT_DATA
    assert results[0]["aux"] == RECORD_OUT_AUX
