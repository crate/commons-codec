import pytest

pytest.importorskip("tikray")

from tikray.model.treatment import Treatment

from commons_codec.model import SQLOperation

pytestmark = pytest.mark.cratedb


@pytest.mark.integration
def test_normalize_list_of_objects(caplog, cratedb):
    """
    Verify writing record to CrateDB, with transformations.
    """

    record_in = {
        "_list_float_int": [{"abc": 42.42}, {"abc": 42}],
        "_list_float_none": [{"id": 1, "abc": 42.42}, {"id": 2, "abc": None}],
        "_list_int_str": [{"abc": 123}, {"abc": "123"}],
    }

    record_out = {
        "_list_float_int": [{"abc": 42.42}, {"abc": 42.0}],
        "_list_float_none": [{"id": 1, "abc": 42.42}, {"id": 2}],
        "_list_int_str": [{"abc": "123"}, {"abc": "123"}],
    }

    # Define CrateDB SQL DDL and DML operations (SQL+parameters).
    operation_ddl = SQLOperation('CREATE TABLE "from".generic (data OBJECT(DYNAMIC))', None)
    operation_dml = SQLOperation('INSERT INTO "from".generic (data) VALUES (:data)', {"data": record_in})

    # Apply treatment to parameters.
    parameters = operation_dml.parameters
    Treatment(normalize_complex_lists=True).apply(parameters)

    # Insert into CrateDB.
    cratedb.database.run_sql(operation_ddl.statement)
    cratedb.database.run_sql(operation_dml.statement, parameters)

    # Verify data in target database.
    assert cratedb.database.refresh_table("from.generic") is True

    results = cratedb.database.run_sql('SELECT * FROM "from".generic;', records=True)  # noqa: S608
    assert results[0]["data"] == record_out
