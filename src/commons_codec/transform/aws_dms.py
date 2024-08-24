# Copyright (c) 2023-2024, The Kotori Developers and contributors.
# Distributed under the terms of the LGPLv3 license, see LICENSE.

# ruff: noqa: S608 FIXME: Possible SQL injection vector through string-based query construction

import logging
import typing as t

import simplejson as json

from commons_codec.exception import MessageFormatError, UnknownOperationError
from commons_codec.model import (
    ColumnType,
    ColumnTypeMapStore,
    PrimaryKeyStore,
    SQLOperation,
    SQLParameterizedClause,
    TableAddress,
)

logger = logging.getLogger(__name__)


class DMSTranslatorCrateDBRecord:
    """
    Translate DMS full-load and cdc events into CrateDB SQL statements.
    """

    # Define name of the column where CDC's record data will get materialized into.
    DATA_COLUMN = "data"

    def __init__(
        self,
        record: t.Dict[str, t.Any],
        container: "DMSTranslatorCrateDB",
    ):
        self.record = record
        self.container = container

        self.metadata: t.Dict[str, t.Any] = self.record.get("metadata", {})
        self.control: t.Dict[str, t.Any] = self.record.get("control", {})
        self.data: t.Dict[str, t.Any] = self.record.get("data", {})

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

    def to_sql(self) -> SQLOperation:
        if self.operation == "create-table":
            pks = self.control.get("table-def", {}).get("primary-key")
            if pks:
                self.primary_keys += pks
            # TODO: What about dropping tables first?
            return SQLOperation(f"CREATE TABLE IF NOT EXISTS {self.address.fqn} ({self.DATA_COLUMN} OBJECT(DYNAMIC));")

        elif self.operation in ["load", "insert"]:
            record = self.record_to_values()
            sql = f"INSERT INTO {self.address.fqn} ({self.DATA_COLUMN}) VALUES (:record);"
            parameters = {"record": record}

        elif self.operation == "update":
            record = self.record_to_values()
            clause = self.update_clause()
            where_clause = self.keys_to_where()
            sql = f"UPDATE {self.address.fqn} SET {clause.set_clause} WHERE {where_clause};"
            parameters = {"record": record}

        elif self.operation == "delete":
            where_clause = self.keys_to_where()
            sql = f"DELETE FROM {self.address.fqn} WHERE {where_clause};"
            parameters = None

        else:
            message = f"Unknown CDC event operation: {self.operation}"
            logger.warning(message)
            raise UnknownOperationError(message, operation=self.operation, record=self.record)

        return SQLOperation(sql, parameters)

    def update_clause(self) -> SQLParameterizedClause:
        """
        Serializes an image to a comma-separated list of column/values pairs
        that can be used in the `SET` clause of an `UPDATE` statement.
        Primary key columns are skipped, since they cannot be updated.

        IN
        {'age': 33, 'attributes': '{"foo": "bar"}', 'id': 42, 'name': 'John'}

        OUT
        data['age'] = '33', data['attributes'] = '{"foo": "bar"}', data['name'] = 'John'
        """
        clause = SQLParameterizedClause()
        for column, value in self.record["data"].items():
            # Skip primary key columns, they cannot be updated
            if column in self.primary_keys:
                continue

            param_name = f":{column}"

            clause.columns.append(f"{self.DATA_COLUMN}['{column}']")
            clause.names.append(param_name)
            clause.values.append(value)  # noqa: PD011

        return clause

    def record_to_values(self) -> t.Dict[str, t.Any]:
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
        return self.data

    def keys_to_where(self) -> str:
        """
        Produce an SQL WHERE clause based on primary key definition and current record's data.
        """
        if not self.primary_keys:
            raise ValueError("Unable to invoke DML operation without primary key information")
        constraints: t.List[str] = []
        for key_name in self.primary_keys:
            key_value = self.data.get(key_name)
            # FIXME: Does the quoting of the value on the right hand side need to take the data type into account?
            constraint = f"{self.DATA_COLUMN}['{key_name}'] = '{key_value}'"
            constraints.append(constraint)
        return " AND ".join(constraints)


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

    def to_sql(self, record: t.Dict[str, t.Any]) -> SQLOperation:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from load|insert|update|delete CDC event record.
        """
        record_decoded = DMSTranslatorCrateDBRecord(record=record, container=self)
        return record_decoded.to_sql()
