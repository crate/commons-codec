# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import copy
import logging
import typing as t

import simplejson as json

from commons_codec.exception import MessageFormatError, UnknownOperationError
from commons_codec.model import (
    ColumnType,
    ColumnTypeMapStore,
    PrimaryKeyStore,
    SQLOperation,
    SQLParameterizedSetClause,
    SQLParameterizedWhereClause,
    TableAddress,
    UniversalRecord,
)

logger = logging.getLogger(__name__)


class DMSTranslatorCrateDBRecord:
    """
    Translate DMS full-load and cdc events into CrateDB SQL statements.
    """

    # Define the name of the column where primary key information will get materialized into.
    # This column uses the `OBJECT(STRICT)` data type.
    PK_COLUMN = "pk"

    # Define the name of the column where CDC's record data will get materialized into.
    # This column uses the `OBJECT(DYNAMIC)` data type.
    TYPED_COLUMN = "data"

    # Define the name of the column where untyped fields will get materialized into.
    # This column uses the `OBJECT(IGNORED)` data type.
    # TODO: Currently not used with DMS.
    UNTYPED_COLUMN = "aux"

    def __init__(
        self,
        event: t.Dict[str, t.Any],
        container: "DMSTranslatorCrateDB",
    ):
        self.event = event
        self.container = container

        self.metadata: t.Dict[str, t.Any] = self.event.get("metadata", {})
        self.control: t.Dict[str, t.Any] = self.event.get("control", {})
        self.data: t.Dict[str, t.Any] = self.event.get("data", {})

        self.operation: t.Union[str, None] = self.metadata.get("operation")

        self.schema: t.Union[str, None] = self.metadata.get("schema-name")
        self.table: t.Union[str, None] = self.metadata.get("table-name")

        # Tweaks.

        # Divert special tables like `awsdms_apply_exceptions` to a dedicated schema.
        # Relevant CDC events are delivered with an empty table name, so some valid
        # name needs to be selected anyway. The outcome of this is that AWS DMS special
        # tables will be created within the sink database, like `dms.awsdms_apply_exceptions`.
        if self.table and self.table.startswith("awsdms_"):
            self.schema = "dms"

        # Sanity checks.
        if not self.metadata or not self.operation:
            message = "Record not in DMS format: metadata and/or operation is missing"
            logger.error(message)
            raise MessageFormatError(message)

        if not self.schema or not self.table:
            message = f"Schema or table name missing or empty: schema={self.schema}, table={self.table}"
            logger.error(message)
            raise MessageFormatError(message)

        self.address: TableAddress = TableAddress(schema=self.schema, table=self.table)

        self.container.primary_keys.setdefault(self.address, [])
        self.container.column_types.setdefault(self.address, {})
        self.primary_keys: t.List[str] = self.container.primary_keys[self.address]
        self.column_types: t.Dict[str, ColumnType] = self.container.column_types[self.address]

        pks = self.control.get("table-def", {}).get("primary-key", [])
        for pk in pks:
            if pk not in self.primary_keys:
                self.primary_keys.append(pk)

    def to_sql(self) -> SQLOperation:
        if self.operation == "create-table":
            return SQLOperation(
                f"CREATE TABLE IF NOT EXISTS {self.address.fqn} ("
                f"{self.PK_COLUMN} OBJECT(STRICT){self.pk_clause()}, "
                f"{self.TYPED_COLUMN} OBJECT(DYNAMIC), "
                f"{self.UNTYPED_COLUMN} OBJECT(IGNORED));"
            )

        elif self.operation == "drop-table":
            # Remove cached schema information by restoring original so a future CREATE starts clean.
            self.container.primary_keys[self.address] = self.container.primary_keys_caller.get(self.address, [])
            self.container.column_types[self.address] = self.container.column_types_caller.get(self.address, {})
            return SQLOperation(f"DROP TABLE IF EXISTS {self.address.fqn};")

        elif self.operation in ["load", "insert"]:
            self.decode_data()
            record = self.decode_record(self.data)
            sql = (
                f"INSERT INTO {self.address.fqn} ("
                f"{self.PK_COLUMN}, "
                f"{self.TYPED_COLUMN}, "
                f"{self.UNTYPED_COLUMN}"
                f") VALUES ("
                f":pk, "
                f":typed, "
                f":untyped) "
                f"ON CONFLICT DO NOTHING;"
            )
            parameters = record.to_dict()

        elif self.operation == "update":
            self.decode_data()
            set_clause = self.update_clause()
            where_clause = self.keys_to_where()
            sql = f"UPDATE {self.address.fqn} SET {set_clause.to_sql()} WHERE {where_clause.to_sql()};"
            parameters = set_clause.values  # noqa: PD011
            parameters.update(where_clause.values)

        elif self.operation == "delete":
            where_clause = self.keys_to_where()
            sql = f"DELETE FROM {self.address.fqn} WHERE {where_clause.to_sql()};"
            parameters = where_clause.values  # noqa: PD011

        else:
            message = f"Unknown CDC event operation: {self.operation}"
            logger.warning(message)
            raise UnknownOperationError(message, operation=self.operation, record=self.event)

        return SQLOperation(sql, parameters)

    def pk_clause(self) -> str:
        """
        Return primary key clause in string format.
        """
        if self.primary_keys:
            columns = self.control.get("table-def", {}).get("columns", {})
            pk_clauses = []
            for pk_name in self.primary_keys:
                col_meta = columns.get(pk_name) or {}
                ltype = col_meta.get("type", "TEXT")
                pk_clauses.append(f'"{pk_name}" {self.resolve_type(ltype)} PRIMARY KEY')
            if pk_clauses:
                return f" AS ({', '.join(pk_clauses)})"
        return ""

    @staticmethod
    def resolve_type(ltype: str) -> str:
        """
        Map DMS/Kinesis data type to CrateDB data type.

        TODO: Right now only the INT* family is mapped. Unrecognised value types are mapped
              to `TEXT`, acting as a sane default. Consider adding an enriched set of type
              mappings when applicable.

        - https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html#CHAP_Target.S3.DataTypes
        - https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html#CHAP_Source-PostgreSQL-DataTypes
        - https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html
        - https://repost.aws/questions/QUkEPhdTIpRoCC7jcQ21xGyQ/amazon-dms-table-mapping-tranformation
        """
        type_map = {
            "INT8": "INT1",
            "INT16": "INT2",
            "INT32": "INT4",
            "INT64": "INT8",
        }
        return type_map.get(ltype, "TEXT")

    def update_clause(self) -> SQLParameterizedSetClause:
        """
        Serializes an image to a comma-separated list of column/values pairs
        that can be used in the `SET` clause of an `UPDATE` statement.
        Primary key columns are skipped, since they cannot be updated.

        IN
        {'age': 33, 'attributes': '{"foo": "bar"}', 'id': 42, 'name': 'John'}

        OUT
        data['age'] = '33', data['attributes'] = '{"foo": "bar"}', data['name'] = 'John'
        """
        clause = SQLParameterizedSetClause()
        for column, value in self.data.items():
            # Skip primary key columns, they cannot be updated.
            if column in self.primary_keys:
                continue
            clause.add(lval=f"{self.TYPED_COLUMN}['{column}']", value=value, name=column)
        return clause

    def decode_data(self):
        """
        Apply type translations to record, and serialize to JSON.

        IN (top-level stripped):
        "data": {"age": 30, "attributes": '{"foo": "bar"}', "id": 42, "name": "John"}

        OUT:
        {"age": 30, "attributes": {"foo": "bar"}, "id": 42, "name": "John"}
        """
        for column_name, column_type in self.column_types.items():
            if column_name in self.data:
                value = self.data[column_name]
                # DMS marshals JSON|JSONB to CLOB, aka. string. Apply a countermeasure.
                if column_type is ColumnType.MAP and isinstance(value, str):
                    value = json.loads(value)
                self.data[column_name] = value

    def decode_record(self, item: t.Dict[str, t.Any], key_names: t.Union[t.List[str], None] = None) -> UniversalRecord:
        """
        Deserialize DMS event record into vanilla Python.
        """
        return UniversalRecord.from_record(item, key_names or self.primary_keys)

    def keys_to_where(self) -> SQLParameterizedWhereClause:
        """
        Produce an SQL WHERE clause based on primary key definition and current record's data.
        """
        if not self.primary_keys:
            raise ValueError("Unable to invoke DML operation without primary key information")
        clause = SQLParameterizedWhereClause()
        for key_name in self.primary_keys:
            key_value = self.data.get(key_name)
            if key_value is not None:
                clause.add(lval=f"{self.PK_COLUMN}['{key_name}']", value=key_value, name=key_name)
        return clause


class DMSTranslatorCrateDB:
    """
    Translate AWS DMS event messages into CrateDB SQL statements that materialize them again.

    The SQL DDL schema for CrateDB:
    CREATE TABLE <tablename> (data OBJECT(DYNAMIC));

    Blueprint:
    https://www.cockroachlabs.com/docs/stable/aws-dms
    """

    def __init__(
        self,
        primary_keys: PrimaryKeyStore = None,
        column_types: ColumnTypeMapStore = None,
    ):
        self.primary_keys = primary_keys or PrimaryKeyStore()
        self.column_types = column_types or ColumnTypeMapStore()

        # Store caller-provided schema information to restore this state on `DROP TABLE` operations.
        self.primary_keys_caller = copy.deepcopy(self.primary_keys)
        self.column_types_caller = copy.deepcopy(self.column_types)

    def to_sql(self, record: t.Dict[str, t.Any]) -> SQLOperation:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from load|insert|update|delete CDC event record.
        """
        record_decoded = DMSTranslatorCrateDBRecord(event=record, container=self)
        return record_decoded.to_sql()
