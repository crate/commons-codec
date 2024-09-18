import pytest

from commons_codec.model import SQLOperation
from commons_codec.transform.mongodb import MongoDBCrateDBConverter, MongoDBFullLoadTranslator
from tests.transform.test_mongodb_data import RECORD_IN_ALL_TYPES, RECORD_OUT_ALL_TYPES


def test_sql_ddl():
    translator = MongoDBFullLoadTranslator(table_name="foo", converter=MongoDBCrateDBConverter())
    assert translator.sql_ddl == "CREATE TABLE IF NOT EXISTS foo (oid TEXT, data OBJECT(DYNAMIC));"


def test_to_sql_operation():
    """
    Verify outcome of `MongoDBFullLoadTranslator.to_sql` operation.
    """
    translator = MongoDBFullLoadTranslator(table_name="foo", converter=MongoDBCrateDBConverter())
    assert translator.to_sql([RECORD_IN_ALL_TYPES]) == SQLOperation(
        statement="INSERT INTO foo (oid, data) VALUES (:oid, :record);",
        parameters=[{"oid": "56027fcae4b09385a85f9344", "record": RECORD_OUT_ALL_TYPES}],
    )


@pytest.mark.integration
def test_to_sql_cratedb(caplog, cratedb):
    """
    Verify writing converted MongoDB document to CrateDB.
    """

    # Compute CrateDB operation (SQL+parameters) from MongoDB document.
    translator = MongoDBFullLoadTranslator(table_name="from.mongodb", converter=MongoDBCrateDBConverter())
    operation = translator.to_sql(RECORD_IN_ALL_TYPES)

    # Insert into CrateDB.
    cratedb.database.run_sql(translator.sql_ddl)
    cratedb.database.run_sql(operation.statement, operation.parameters)

    # Verify data in target database.
    assert cratedb.database.table_exists("from.mongodb") is True
    assert cratedb.database.refresh_table("from.mongodb") is True
    assert cratedb.database.count_records("from.mongodb") == 1

    results = cratedb.database.run_sql('SELECT * FROM "from".mongodb;', records=True)  # noqa: S608
    assert results[0]["data"] == RECORD_OUT_ALL_TYPES
