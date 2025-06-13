# ruff: noqa: E402
from copy import deepcopy

import pytest

pytest.importorskip("tikray")

from tikray.model.collection import CollectionTransformation
from tikray.model.moksha import MokshaTransformation

pytestmark = pytest.mark.mongodb

from commons_codec.model import SQLOperation
from commons_codec.transform.mongodb import MongoDBCrateDBConverter, MongoDBFullLoadTranslator
from tests.transform.mongodb.data import (
    RECORD_IN_ALL_TYPES,
    RECORD_IN_ANOMALIES,
    RECORD_OUT_ALL_TYPES,
    RECORD_OUT_ANOMALIES,
)

testdata = [
    (RECORD_IN_ALL_TYPES, RECORD_OUT_ALL_TYPES, "all-types"),
    (RECORD_IN_ANOMALIES, RECORD_OUT_ANOMALIES, "anomalies"),
]
testdata_ids = [
    "all-types",
    "anomalies",
]


def test_sql_ddl():
    translator = MongoDBFullLoadTranslator(table_name="foo")
    assert translator.sql_ddl == "CREATE TABLE IF NOT EXISTS foo (oid TEXT, data OBJECT(DYNAMIC));"


def make_translator(kind: str) -> MongoDBFullLoadTranslator:
    transformation = None
    if kind == "anomalies":
        transformation = CollectionTransformation(
            pre=MokshaTransformation()
            .jq(".[] |= (.python.list_of_nested_list |= flatten)")
            .jq(".[] |= (.python.list_of_objects |= prune_array_of_objects)")
            .jq('.[] |= (.python.to_dict |= to_object({"key": "id"}))')
            .jq(".[] |= (.python.to_list |= to_array)")
            .jq(".[] |= (.python.to_string |= tostring)")
        )
    converter = MongoDBCrateDBConverter(
        timestamp_to_epoch=True,
        timestamp_use_milliseconds=True,
        transformation=transformation,
    )
    translator = MongoDBFullLoadTranslator(table_name="from.mongodb", converter=converter)
    return translator


@pytest.mark.parametrize("data_in, data_out, kind", testdata, ids=testdata_ids)
def test_to_sql_operation(data_in, data_out, kind):
    """
    Verify outcome of `MongoDBFullLoadTranslator.to_sql` operation.
    """
    # Create translator component.
    translator = make_translator(kind)

    # Compute CrateDB operation (SQL+parameters) from MongoDB document.
    operation = translator.to_sql(deepcopy([data_in]))
    assert operation == SQLOperation(
        statement='INSERT INTO "from".mongodb (oid, data) VALUES (:oid, :record);',
        parameters=[{"oid": "56027fcae4b09385a85f9344", "record": data_out}],
    )


@pytest.mark.integration
@pytest.mark.parametrize("data_in, data_out, kind", testdata, ids=testdata_ids)
def test_to_sql_cratedb(caplog, cratedb, data_in, data_out, kind):
    """
    Verify writing converted MongoDB document to CrateDB.
    """

    # Create translator component.
    translator = make_translator(kind)

    # Compute CrateDB operation (SQL+parameters) from MongoDB document.
    operation = translator.to_sql(deepcopy(data_in))

    # Insert into CrateDB.
    cratedb.database.run_sql(translator.sql_ddl)
    cratedb.database.run_sql(operation.statement, operation.parameters)

    # Verify data in target database.
    assert cratedb.database.table_exists("from.mongodb") is True
    assert cratedb.database.refresh_table("from.mongodb") is True
    assert cratedb.database.count_records("from.mongodb") == 1

    results = cratedb.database.run_sql('SELECT * FROM "from".mongodb;', records=True)  # noqa: S608
    assert results[0]["data"] == data_out
