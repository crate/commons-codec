from copy import deepcopy

import pytest

from commons_codec.exception import MessageFormatError, UnknownOperationError
from commons_codec.model import (
    ColumnMappingStrategy,
    ColumnType,
    ColumnTypeMapStore,
    PrimaryKeyStore,
    SkipOperation,
    SQLOperation,
    TableAddress,
)
from commons_codec.transform.aws_dms import DMSTranslatorCrateDB
from tests.transform.test_aws_dms_universal import (
    MSG_CONTROL_AWSDMS,
    MSG_CONTROL_CREATE_TABLE,
    MSG_CONTROL_DROP_TABLE,
    MSG_DATA_DELETE,
    MSG_DATA_INSERT,
    MSG_DATA_UPDATE_VALUE,
    MSG_SCHEMA_TABLE_MISSING,
    MSG_UNKNOWN_OPERATION,
    MSG_UNKNOWN_SHAPE,
    RECORD_INSERT,
    RECORD_UPDATE,
)


@pytest.fixture
def cdc_direct():
    """
    Provide a regular translator instance.
    """
    ta_dms = TableAddress(schema="dms", table="awsdms_apply_exceptions")
    ta_foo = TableAddress(schema="public", table="foo")
    column_types = ColumnTypeMapStore().add(
        table=ta_foo,
        column="attributes",
        type_=ColumnType.OBJECT,
    )
    mapping_strategy = {
        ta_foo: ColumnMappingStrategy.DIRECT,
        ta_dms: ColumnMappingStrategy.DIRECT,
    }
    return DMSTranslatorCrateDB(column_types=column_types, mapping_strategy=mapping_strategy)


@pytest.fixture
def cdc_direct_without_ddl():
    """
    Provide a translator instance that ignores DDL events.
    """
    ta = TableAddress(schema="public", table="foo")
    primary_keys = PrimaryKeyStore()
    primary_keys[ta] = {"name": "id", "type": "INTEGER"}
    column_types = ColumnTypeMapStore().add(
        table=ta,
        column="attributes",
        type_=ColumnType.OBJECT,
    )
    mapping_strategy = {ta: ColumnMappingStrategy.DIRECT}
    ignore_ddl = {ta: True}
    return DMSTranslatorCrateDB(column_types=column_types, mapping_strategy=mapping_strategy, ignore_ddl=ignore_ddl)


def test_direct_decode_cdc_unknown_source(cdc_direct):
    with pytest.raises(MessageFormatError) as ex:
        cdc_direct.to_sql(MSG_UNKNOWN_SHAPE)
    assert ex.match("Record not in DMS format: metadata and/or operation is missing")


def test_direct_decode_cdc_missing_schema_or_table(cdc_direct):
    with pytest.raises(MessageFormatError) as ex:
        cdc_direct.to_sql(MSG_SCHEMA_TABLE_MISSING)
    assert ex.match("Schema or table name missing or empty: schema=None, table=None")


def test_direct_decode_cdc_unknown_event(cdc_direct):
    with pytest.raises(UnknownOperationError) as ex:
        cdc_direct.to_sql(MSG_UNKNOWN_OPERATION)
    assert ex.match("DMS CDC event operation unknown: FOOBAR")
    assert ex.value.operation == "FOOBAR"
    assert ex.value.record.event == {
        "control": {},
        "metadata": {"operation": "FOOBAR", "schema-name": "public", "table-name": "foo"},
    }


def test_direct_decode_cdc_sql_ddl_regular_create(cdc_direct):
    assert cdc_direct.to_sql(MSG_CONTROL_CREATE_TABLE) == SQLOperation(
        statement="CREATE TABLE IF NOT EXISTS public.foo "
        '("age" INT4, "attributes" TEXT, "id" INT4 PRIMARY KEY, "name" TEXT);',
        parameters=None,
    )


def test_direct_decode_cdc_sql_ddl_regular_drop(cdc_direct):
    assert cdc_direct.to_sql(MSG_CONTROL_DROP_TABLE) == SQLOperation(
        statement="DROP TABLE IF EXISTS public.foo;",
        parameters=None,
    )


def test_direct_decode_cdc_sql_without_ddl_regular_create(cdc_direct_without_ddl):
    with pytest.raises(SkipOperation) as ex:
        cdc_direct_without_ddl.to_sql(MSG_CONTROL_CREATE_TABLE)
    assert ex.match("Ignoring DMS DDL event: create-table")


def test_direct_decode_cdc_sql_without_ddl_regular_drop(cdc_direct_without_ddl):
    with pytest.raises(SkipOperation) as ex:
        cdc_direct_without_ddl.to_sql(MSG_CONTROL_DROP_TABLE)
    assert ex.match("Ignoring DMS DDL event: drop-table")


def test_direct_decode_cdc_sql_ddl_awsdms(cdc_direct):
    assert cdc_direct.to_sql(MSG_CONTROL_AWSDMS) == SQLOperation(
        statement="CREATE TABLE IF NOT EXISTS dms.awsdms_apply_exceptions "
        '("ERROR" TEXT, "ERROR_TIME" TEXT, "STATEMENT" TEXT, "TABLE_NAME" TEXT, "TABLE_OWNER" TEXT, "TASK_NAME" TEXT);',
        parameters=None,
    )


def test_direct_decode_cdc_insert_without_pk(cdc_direct):
    """
    Emulate INSERT operation without primary keys.
    """
    assert cdc_direct.to_sql(MSG_DATA_INSERT) == SQLOperation(
        statement="INSERT INTO public.foo "
        '("age", "attributes", "id", "name") VALUES '
        "(:age, :attributes, :id, :name) ON CONFLICT DO NOTHING;",
        parameters={"age": 31, "attributes": {"baz": "qux"}, "id": 46, "name": "Jane"},
    )


def test_direct_decode_cdc_insert_with_pk(cdc_direct):
    """
    Emulate INSERT operation with primary keys.
    """
    # Seed translator with a control message, describing the table schema.
    cdc_direct.to_sql(MSG_CONTROL_CREATE_TABLE)

    # Emulate an INSERT operation.
    record = deepcopy(RECORD_INSERT)
    record.pop("id")
    assert cdc_direct.to_sql(MSG_DATA_INSERT) == SQLOperation(
        statement="INSERT INTO public.foo "
        '("age", "attributes", "id", "name") VALUES '
        "(:age, :attributes, :id, :name) ON CONFLICT DO NOTHING;",
        parameters={"age": 31, "attributes": {"baz": "qux"}, "id": 46, "name": "Jane"},
    )


def test_direct_decode_cdc_update_success(cdc_direct):
    """
    Update statements need schema knowledge about primary keys.
    """
    # Seed translator with a control message, describing the table schema.
    cdc_direct.to_sql(MSG_CONTROL_CREATE_TABLE)

    # Emulate an UPDATE operation.
    assert cdc_direct.to_sql(MSG_DATA_UPDATE_VALUE) == SQLOperation(
        statement="UPDATE public.foo SET age=:age, attributes=:attributes, name=:name WHERE id=:id;",
        parameters=RECORD_UPDATE,
    )


def test_direct_decode_cdc_update_failure():
    """
    Update statements without schema knowledge are not possible.

    When no `create-table` statement has been processed yet,
    the machinery doesn't know about primary keys.
    """
    # Emulate an UPDATE operation without seeding the translator.
    ta = TableAddress(schema="public", table="foo")
    mapping_strategy = {
        ta: ColumnMappingStrategy.DIRECT,
    }
    with pytest.raises(ValueError) as ex:
        DMSTranslatorCrateDB(mapping_strategy=mapping_strategy).to_sql(MSG_DATA_UPDATE_VALUE)
    assert ex.match("Unable to invoke DML operation without primary key information")


def test_direct_decode_cdc_delete_success(cdc_direct):
    """
    Delete statements need schema knowledge about primary keys.
    """
    # Seed translator with control message, describing the table schema.
    cdc_direct.to_sql(MSG_CONTROL_CREATE_TABLE)

    # Emulate a DELETE operation.
    assert cdc_direct.to_sql(MSG_DATA_DELETE) == SQLOperation(
        statement="DELETE FROM public.foo WHERE id=:id;", parameters={"id": 45}
    )


def test_direct_decode_cdc_delete_failure():
    """
    Delete statements without schema knowledge are not possible.

    When no `create-table` statement has been processed yet,
    the machinery doesn't know about primary keys.
    """
    # Emulate an DELETE operation without seeding the translator.
    ta = TableAddress(schema="public", table="foo")
    mapping_strategy = {
        ta: ColumnMappingStrategy.DIRECT,
    }
    with pytest.raises(ValueError) as ex:
        DMSTranslatorCrateDB(mapping_strategy=mapping_strategy).to_sql(MSG_DATA_DELETE)
    assert ex.match("Unable to invoke DML operation without primary key information")
