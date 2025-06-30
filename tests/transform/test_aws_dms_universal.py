# ruff: noqa: S608 FIXME: Possible SQL injection vector through string-based query construction
import base64
import json
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

RECORD_INSERT = {"age": 31, "attributes": {"baz": "qux"}, "id": 46, "name": "Jane"}
RECORD_UPDATE = {"age": 33, "attributes": {"foo": "bar"}, "id": 42, "name": "John"}

MSG_UNKNOWN_SHAPE = {
    "unknown": "foo:bar",
}
MSG_SCHEMA_TABLE_MISSING = {
    "control": {},
    "metadata": {
        "operation": "insert",
    },
}
MSG_UNKNOWN_OPERATION = {
    "control": {},
    "metadata": {
        "operation": "FOOBAR",
        "schema-name": "public",
        "table-name": "foo",
    },
}

MSG_CONTROL_DROP_TABLE = {
    "control": {},
    "metadata": {
        "operation": "drop-table",
        "partition-key-type": "task-id",
        "partition-key-value": "serv-res-id-1722195358878-yhru",
        "record-type": "control",
        "schema-name": "public",
        "table-name": "foo",
        "timestamp": "2024-07-29T00:30:47.258815Z",
    },
}

MSG_CONTROL_CREATE_TABLE = {
    "control": {
        "table-def": {
            "columns": {
                "age": {"nullable": True, "type": "INT32"},
                "attributes": {"nullable": True, "type": "STRING"},
                "id": {"nullable": False, "type": "INT32"},
                "name": {"nullable": True, "type": "STRING"},
            },
            "primary-key": ["id"],
        }
    },
    "metadata": {
        "operation": "create-table",
        "partition-key-type": "task-id",
        "partition-key-value": "serv-res-id-1722195358878-yhru",
        "record-type": "control",
        "schema-name": "public",
        "table-name": "foo",
        "timestamp": "2024-07-29T00:30:47.266581Z",
    },
}

MSG_DATA_LOAD = {
    "data": {"age": 30, "attributes": '{"foo": "bar"}', "id": 42, "name": "John"},
    "metadata": {
        "operation": "load",
        "partition-key-type": "primary-key",
        "partition-key-value": "public.foo.42",
        "record-type": "data",
        "schema-name": "public",
        "table-name": "foo",
        "timestamp": "2024-07-29T00:57:35.691762Z",
    },
}

MSG_DATA_INSERT = {
    "data": {"age": 31, "attributes": '{"baz": "qux"}', "id": 46, "name": "Jane"},
    "metadata": {
        "commit-timestamp": "2024-07-29T00:58:17.974340Z",
        "operation": "insert",
        "partition-key-type": "schema-table",
        "record-type": "data",
        "schema-name": "public",
        "stream-position": "00000002/7C007178.3.00000002/7C007178",
        "table-name": "foo",
        "timestamp": "2024-07-29T00:58:17.983670Z",
        "transaction-id": 1139,
        "transaction-record-id": 1,
    },
}

MSG_DATA_UPDATE_VALUE = {
    "before-image": {},
    "data": {"age": 33, "attributes": '{"foo": "bar"}', "id": 42, "name": "John"},
    "metadata": {
        "commit-timestamp": "2024-07-29T00:58:44.886717Z",
        "operation": "update",
        "partition-key-type": "schema-table",
        "prev-transaction-id": 1139,
        "prev-transaction-record-id": 1,
        "record-type": "data",
        "schema-name": "public",
        "stream-position": "00000002/7C007328.2.00000002/7C007328",
        "table-name": "foo",
        "timestamp": "2024-07-29T00:58:44.895275Z",
        "transaction-id": 1140,
        "transaction-record-id": 1,
    },
}

MSG_DATA_UPDATE_PK = {
    "before-image": {"id": 46},
    "data": {"age": 31, "attributes": '{"baz": "qux"}', "id": 45, "name": "Jane"},
    "metadata": {
        "commit-timestamp": "2024-07-29T00:59:07.678294Z",
        "operation": "update",
        "partition-key-type": "schema-table",
        "prev-transaction-id": 1140,
        "prev-transaction-record-id": 1,
        "record-type": "data",
        "schema-name": "public",
        "stream-position": "00000002/7C0073F8.2.00000002/7C0073F8",
        "table-name": "foo",
        "timestamp": "2024-07-29T00:59:07.686557Z",
        "transaction-id": 1141,
        "transaction-record-id": 1,
    },
}

MSG_DATA_DELETE = {
    "data": {"age": None, "attributes": None, "id": 45, "name": None},
    "metadata": {
        "commit-timestamp": "2024-07-29T01:09:25.366257Z",
        "operation": "delete",
        "partition-key-type": "schema-table",
        "prev-transaction-id": 1141,
        "prev-transaction-record-id": 1,
        "record-type": "data",
        "schema-name": "public",
        "stream-position": "00000002/840001D8.2.00000002/840001D8",
        "table-name": "foo",
        "timestamp": "2024-07-29T01:09:25.375525Z",
        "transaction-id": 1144,
        "transaction-record-id": 1,
    },
}

MSG_CONTROL_AWSDMS = {
    "control": {
        "table-def": {
            "columns": {
                "ERROR": {"nullable": False, "type": "STRING"},
                "ERROR_TIME": {"nullable": False, "type": "TIMESTAMP"},
                "STATEMENT": {"nullable": False, "type": "STRING"},
                "TABLE_NAME": {"length": 128, "nullable": False, "type": "STRING"},
                "TABLE_OWNER": {"length": 128, "nullable": False, "type": "STRING"},
                "TASK_NAME": {"length": 128, "nullable": False, "type": "STRING"},
            }
        }
    },
    "metadata": {
        "operation": "create-table",
        "partition-key-type": "task-id",
        "partition-key-value": "7QBLNBTPCNDEBG7CHI3WA73YFA",
        "record-type": "control",
        "schema-name": "",
        "table-name": "awsdms_apply_exceptions",
        "timestamp": "2024-08-04T10:50:10.584772Z",
    },
}


@pytest.fixture
def cdc_universal():
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
        ta_foo: ColumnMappingStrategy.UNIVERSAL,
        ta_dms: ColumnMappingStrategy.UNIVERSAL,
    }
    return DMSTranslatorCrateDB(column_types=column_types, mapping_strategy=mapping_strategy)


@pytest.fixture
def cdc_universal_without_ddl():
    """
    Provide a translator instance that ignores DDL events.
    """
    ta_dms = TableAddress(schema="dms", table="awsdms_apply_exceptions")
    ta_foo = TableAddress(schema="public", table="foo")
    primary_keys = PrimaryKeyStore()
    primary_keys[ta_foo] = {"name": "id", "type": "INTEGER"}
    column_types = ColumnTypeMapStore().add(
        table=ta_foo,
        column="attributes",
        type_=ColumnType.OBJECT,
    )
    mapping_strategy = {
        ta_foo: ColumnMappingStrategy.UNIVERSAL,
        ta_dms: ColumnMappingStrategy.UNIVERSAL,
    }
    ignore_ddl = {ta_foo: True}
    return DMSTranslatorCrateDB(column_types=column_types, mapping_strategy=mapping_strategy, ignore_ddl=ignore_ddl)


def test_unknown_strategy(cdc_universal):
    ta = TableAddress(schema="public", table="foo")
    mapping_strategy = {
        ta: "unknown",
    }
    with pytest.raises(ValueError) as ex:
        dtc = DMSTranslatorCrateDB(mapping_strategy=mapping_strategy)
        dtc.to_sql(MSG_DATA_LOAD)
    assert ex.match("'UNKNOWN' is not a valid ColumnMappingStrategy")


def test_universal_decode_cdc_unknown_source(cdc_universal):
    with pytest.raises(MessageFormatError) as ex:
        cdc_universal.to_sql(MSG_UNKNOWN_SHAPE)
    assert ex.match("Record not in DMS format: metadata and/or operation is missing")


def test_universal_decode_cdc_missing_schema_or_table(cdc_universal):
    with pytest.raises(MessageFormatError) as ex:
        cdc_universal.to_sql(MSG_SCHEMA_TABLE_MISSING)
    assert ex.match("Schema or table name missing or empty: schema=None, table=None")


def test_universal_decode_cdc_unknown_event(cdc_universal):
    with pytest.raises(UnknownOperationError) as ex:
        cdc_universal.to_sql(MSG_UNKNOWN_OPERATION)
    assert ex.match("DMS CDC event operation unknown: FOOBAR")
    assert ex.value.operation == "FOOBAR"
    assert ex.value.record.event == {
        "control": {},
        "metadata": {"operation": "FOOBAR", "schema-name": "public", "table-name": "foo"},
    }


def test_universal_decode_cdc_drop_resets_schema_cache(cdc_universal):
    # Create with single-PK.
    cdc_universal.to_sql(MSG_CONTROL_CREATE_TABLE)
    # Drop table – cache must be cleared.
    cdc_universal.to_sql(MSG_CONTROL_DROP_TABLE)
    # Recreate with a different PK set.
    msg_recreate = json.loads(json.dumps(MSG_CONTROL_CREATE_TABLE))
    msg_recreate["control"]["table-def"]["primary-key"] = ["name"]
    op = cdc_universal.to_sql(msg_recreate)
    assert '("name" TEXT PRIMARY KEY)' in op.statement


def test_universal_decode_cdc_sql_ddl_regular_create(cdc_universal):
    assert cdc_universal.to_sql(MSG_CONTROL_CREATE_TABLE) == SQLOperation(
        statement="CREATE TABLE IF NOT EXISTS public.foo "
        '(pk OBJECT(STRICT) AS ("id" INT4 PRIMARY KEY), data OBJECT(DYNAMIC), aux OBJECT(IGNORED));',
        parameters=None,
    )


def test_universal_decode_cdc_sql_ddl_regular_drop(cdc_universal):
    assert cdc_universal.to_sql(MSG_CONTROL_DROP_TABLE) == SQLOperation(
        statement="DROP TABLE IF EXISTS public.foo;",
        parameters=None,
    )


def test_universal_decode_cdc_sql_without_ddl_regular_create(cdc_universal_without_ddl):
    with pytest.raises(SkipOperation) as ex:
        cdc_universal_without_ddl.to_sql(MSG_CONTROL_CREATE_TABLE)
    assert ex.match("Ignoring DMS DDL event: create-table")


def test_universal_decode_cdc_sql_without_ddl_regular_drop(cdc_universal_without_ddl):
    with pytest.raises(SkipOperation) as ex:
        cdc_universal_without_ddl.to_sql(MSG_CONTROL_DROP_TABLE)
    assert ex.match("Ignoring DMS DDL event: drop-table")


def test_universal_decode_cdc_sql_ddl_awsdms(cdc_universal):
    assert cdc_universal.to_sql(MSG_CONTROL_AWSDMS) == SQLOperation(
        statement="CREATE TABLE IF NOT EXISTS dms.awsdms_apply_exceptions "
        "(pk OBJECT(STRICT), data OBJECT(DYNAMIC), aux OBJECT(IGNORED));",
        parameters=None,
    )


def test_universal_decode_cdc_insert_without_pk(cdc_universal):
    """
    Emulate INSERT operation without primary keys.
    """
    assert cdc_universal.to_sql(MSG_DATA_INSERT) == SQLOperation(
        statement="INSERT INTO public.foo (pk, data, aux) VALUES (:pk, :typed, :untyped) ON CONFLICT DO NOTHING;",
        parameters={"pk": {}, "typed": RECORD_INSERT, "untyped": {}},
    )


def test_universal_decode_cdc_insert_with_pk(cdc_universal):
    """
    Emulate INSERT operation with primary keys.
    """
    # Seed translator with a control message, describing the table schema.
    cdc_universal.to_sql(MSG_CONTROL_CREATE_TABLE)

    # Emulate an INSERT operation.
    record = deepcopy(RECORD_INSERT)
    record.pop("id")
    assert cdc_universal.to_sql(MSG_DATA_INSERT) == SQLOperation(
        statement="INSERT INTO public.foo (pk, data, aux) VALUES (:pk, :typed, :untyped) ON CONFLICT DO NOTHING;",
        parameters={"pk": {"id": 46}, "typed": record, "untyped": {}},
    )


def test_universal_decode_cdc_update_success(cdc_universal):
    """
    Update statements need schema knowledge about primary keys.
    """
    # Seed translator with a control message, describing the table schema.
    cdc_universal.to_sql(MSG_CONTROL_CREATE_TABLE)

    # Emulate an UPDATE operation.
    assert cdc_universal.to_sql(MSG_DATA_UPDATE_VALUE) == SQLOperation(
        statement="UPDATE public.foo SET "
        "data['age']=:age, data['attributes']=:attributes, data['name']=:name "
        "WHERE pk['id']=:id;",
        parameters=RECORD_UPDATE,
    )


def test_universal_decode_cdc_update_failure():
    """
    Update statements without schema knowledge are not possible.

    When no `create-table` statement has been processed yet,
    the machinery doesn't know about primary keys.
    """
    ta = TableAddress(schema="public", table="foo")
    mapping_strategy = {
        ta: ColumnMappingStrategy.UNIVERSAL,
    }
    # Emulate an UPDATE operation without seeding the translator.
    with pytest.raises(ValueError) as ex:
        DMSTranslatorCrateDB(mapping_strategy=mapping_strategy).to_sql(MSG_DATA_UPDATE_VALUE)
    assert ex.match("Unable to invoke DML operation without primary key information")


def test_universal_decode_cdc_delete_success(cdc_universal):
    """
    Delete statements need schema knowledge about primary keys.
    """
    # Seed translator with control message, describing the table schema.
    cdc_universal.to_sql(MSG_CONTROL_CREATE_TABLE)

    # Emulate the DELETE operation.
    assert cdc_universal.to_sql(MSG_DATA_DELETE) == SQLOperation(
        statement="DELETE FROM public.foo WHERE pk['id']=:id;", parameters={"id": 45}
    )


def test_universal_decode_cdc_delete_failure():
    """
    Delete statements without schema knowledge are not possible.

    When no `create-table` statement has been processed yet,
    the machinery doesn't know about primary keys.
    """
    ta = TableAddress(schema="public", table="foo")
    mapping_strategy = {
        ta: ColumnMappingStrategy.UNIVERSAL,
    }
    # Emulate an DELETE operation without seeding the translator.
    with pytest.raises(ValueError) as ex:
        DMSTranslatorCrateDB(mapping_strategy=mapping_strategy).to_sql(MSG_DATA_DELETE)
    assert ex.match("Unable to invoke DML operation without primary key information")


if __name__ == "__main__":
    print(base64.b64encode(json.dumps(MSG_DATA_INSERT).encode("utf-8")))  # noqa: T201
