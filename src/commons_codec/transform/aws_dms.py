# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import abc
import copy
import logging
import typing as t

import simplejson as json
from attr import define

from commons_codec.exception import MessageFormatError, UnknownOperationError
from commons_codec.model import (
    ColumnMappingStrategy,
    ColumnType,
    ColumnTypeMapStore,
    PrimaryKeyStore,
    SkipOperation,
    SQLOperation,
    SQLParameterizedSetClause,
    SQLParameterizedWhereClause,
    TableAddress,
    UniversalRecord,
)

logger = logging.getLogger(__name__)


@define
class DMSBucket:
    """
    Decode and transport DMS events.
    """

    event: t.Dict[str, t.Any]

    def __attrs_post_init__(self):
        # Tweaks.

        # Divert special tables like `awsdms_apply_exceptions` to a dedicated schema.
        # Relevant CDC events are delivered with an empty table name, so some valid
        # name needs to be selected anyway. The outcome of this is that AWS DMS special
        # tables will be created within the sink database, like `dms.awsdms_apply_exceptions`.
        if self.table and self.table.startswith("awsdms_"):
            self.metadata["schema-name"] = "dms"

        # Sanity checks.
        if not self.metadata or not self.operation:
            message = "Record not in DMS format: metadata and/or operation is missing"
            logger.error(message)
            raise MessageFormatError(message)

        if not self.schema or not self.table:
            message = f"Schema or table name missing or empty: schema={self.schema}, table={self.table}"
            logger.error(message)
            raise MessageFormatError(message)

    @property
    def address(self) -> TableAddress:
        return TableAddress(schema=t.cast(str, self.schema), table=t.cast(str, self.table))

    @property
    def metadata(self) -> t.Dict[str, t.Any]:
        return self.event.get("metadata", {})

    @property
    def control(self) -> t.Dict[str, t.Any]:
        return self.event.get("control", {})

    @property
    def data(self) -> t.Dict[str, t.Any]:
        return self.event.get("data", {})

    @property
    def operation(self) -> t.Union[str, None]:
        return self.metadata.get("operation")

    @property
    def schema(self) -> t.Union[str, None]:
        return self.metadata.get("schema-name")

    @property
    def table(self) -> t.Union[str, None]:
        return self.metadata.get("table-name")


class DMSTranslatorCrateDBRecordFactory:
    """
    Entrypoint factory for creating variants of `DMSTranslatorCrateDBRecord`.
    """

    DEFAULT_MAPPING_STRATEGY = ColumnMappingStrategy.DIRECT

    @classmethod
    def create(
        cls,
        event: t.Dict[str, t.Any],
        container: "DMSTranslatorCrateDB",
    ) -> "DMSTranslatorCrateDBRecordBase":
        bucket = DMSBucket(event=event)
        mapping_strategy = cls.DEFAULT_MAPPING_STRATEGY
        if bucket.address:
            val = container.mapping_strategy.get(bucket.address, cls.DEFAULT_MAPPING_STRATEGY.value)
            if isinstance(val, str):
                mapping_strategy = ColumnMappingStrategy(val.upper())
            else:
                mapping_strategy = val

        if mapping_strategy is ColumnMappingStrategy.DIRECT:
            return DMSTranslatorCrateDBRecordDirect(bucket=bucket, container=container)
        if mapping_strategy is ColumnMappingStrategy.UNIVERSAL:
            return DMSTranslatorCrateDBRecordUniversal(bucket=bucket, container=container)

        raise TypeError(
            f"Column mapping strategy unknown: {mapping_strategy}. Expected DIRECT or UNIVERSAL."
        )  # pragma: no cover


class DMSTranslatorCrateDBRecordBase(abc.ABC):
    """
    Abstract base class for multiple variants of `DMSTranslatorCrateDBRecord`.
    """

    def __init__(
        self,
        bucket: DMSBucket,
        container: "DMSTranslatorCrateDB",
    ):
        self.bucket = bucket
        self.container = container

        self.address = self.bucket.address

        self.container.primary_keys.setdefault(self.address, [])
        self.container.column_types.setdefault(self.address, {})
        self.container.ignore_ddl.setdefault(self.address, False)
        self.primary_keys: t.List[str] = self.container.primary_keys[self.address]
        self.column_types: t.Dict[str, ColumnType] = self.container.column_types[self.address]
        self.ignore_ddl: bool = self.container.ignore_ddl[self.address]

        if not self.ignore_ddl:
            pks = self.bucket.control.get("table-def", {}).get("primary-key", [])
            for pk in pks:
                if pk not in self.primary_keys:
                    self.primary_keys.append(pk)

    def to_sql(self) -> SQLOperation:
        operation = self.bucket.operation
        if operation == "create-table":
            return self.create_operation()

        if operation == "drop-table":
            return self.drop_operation()

        if operation in ["load", "insert"]:
            return self.insert_operation()

        if operation == "update":
            return self.update_operation()

        if operation == "delete":
            return self.delete_operation()

        # Default case for unknown operations
        message = f"DMS CDC event operation unknown: {operation}"
        logger.warning(message)
        raise UnknownOperationError(message, operation=operation, record=self.bucket)

    @abc.abstractmethod
    def create_operation(self) -> SQLOperation:
        raise NotImplementedError("Must be implemented by subclass")

    def drop_operation(self) -> SQLOperation:
        if self.ignore_ddl:
            raise SkipOperation("Ignoring DMS DDL event: drop-table")
        # Remove cached schema information by restoring original so a future CREATE starts clean.
        self.container.primary_keys[self.address] = self.container.primary_keys_caller.get(self.address, [])
        self.container.column_types[self.address] = self.container.column_types_caller.get(self.address, {})
        return SQLOperation(f"DROP TABLE IF EXISTS {self.address.fqn};")

    @abc.abstractmethod
    def insert_operation(self) -> SQLOperation:
        raise NotImplementedError("Must be implemented by subclass")

    def update_operation(self) -> SQLOperation:
        self.decode_data()
        set_clause = self.update_clause()
        where_clause = self.keys_to_where()
        sql = f"UPDATE {self.address.fqn} SET {set_clause.to_sql()} WHERE {where_clause.to_sql()};"
        parameters = set_clause.values  # noqa: PD011
        parameters.update(where_clause.values)
        return SQLOperation(sql, parameters)

    def delete_operation(self) -> SQLOperation:
        where_clause = self.keys_to_where()
        sql = f"DELETE FROM {self.address.fqn} WHERE {where_clause.to_sql()};"
        parameters = where_clause.values  # noqa: PD011
        return SQLOperation(sql, parameters)

    @abc.abstractmethod
    def update_clause(self) -> SQLParameterizedSetClause:
        raise NotImplementedError("Must be implemented by subclass")

    @abc.abstractmethod
    def keys_to_where(self) -> SQLParameterizedWhereClause:
        raise NotImplementedError("Must be implemented by subclass")

    def decode_data(self):
        """
        Apply type translations to record, and serialize to JSON.

        IN (top-level stripped):
        "data": {"age": 30, "attributes": '{"foo": "bar"}', "id": 42, "name": "John"}

        OUT:
        {"age": 30, "attributes": {"foo": "bar"}, "id": 42, "name": "John"}
        """
        data = self.bucket.data
        for column_name, column_type in self.column_types.items():
            if column_name in data:
                value = data[column_name]
                # DMS marshals JSON|JSONB to CLOB, aka. string. Apply a countermeasure.
                if (column_type is ColumnType.MAP or column_type is ColumnType.OBJECT) and isinstance(value, str):
                    value = json.loads(value)
                data[column_name] = value

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


class DMSTranslatorCrateDBRecordUniversal(DMSTranslatorCrateDBRecordBase):
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
    UNTYPED_COLUMN = "aux"

    def create_operation(self) -> SQLOperation:
        if self.ignore_ddl:
            raise SkipOperation("Ignoring DMS DDL event: create-table")
        return SQLOperation(
            f"CREATE TABLE IF NOT EXISTS {self.address.fqn} ("
            f"{self.PK_COLUMN} OBJECT(STRICT){self.pk_clause()}, "
            f"{self.TYPED_COLUMN} OBJECT(DYNAMIC), "
            f"{self.UNTYPED_COLUMN} OBJECT(IGNORED));"
        )

    def insert_operation(self) -> SQLOperation:
        self.decode_data()
        record = self.decode_record(self.bucket.data)
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
        return SQLOperation(sql, parameters)

    def pk_clause(self) -> str:
        """
        Return primary key clause in string format.
        """
        if self.primary_keys:
            columns = self.bucket.control.get("table-def", {}).get("columns", {})
            pk_clauses = []
            for pk_name in self.primary_keys:
                col_meta = columns.get(pk_name) or {}
                ltype = col_meta.get("type", "TEXT")
                pk_clauses.append(f'"{pk_name}" {self.resolve_type(ltype)} PRIMARY KEY')
            if pk_clauses:
                return f" AS ({', '.join(pk_clauses)})"
        return ""

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
        data = self.bucket.data
        clause = SQLParameterizedSetClause()
        for column, value in data.items():
            # Skip primary key columns, they cannot be updated.
            if column in self.primary_keys:
                continue
            clause.add(lval=f"{self.TYPED_COLUMN}['{column}']", value=value, name=column)
        return clause

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
        data = self.bucket.data
        for key_name in self.primary_keys:
            key_value = data.get(key_name)
            if key_value is not None:
                clause.add(lval=f"{self.PK_COLUMN}['{key_name}']", value=key_value, name=key_name)
        return clause


class DMSTranslatorCrateDBRecordDirect(DMSTranslatorCrateDBRecordBase):
    def create_operation(self) -> SQLOperation:
        if self.ignore_ddl:
            raise SkipOperation("Ignoring DMS DDL event: create-table")
        return SQLOperation(f"CREATE TABLE IF NOT EXISTS {self.address.fqn} ({', '.join(self.columns_ddl())});")

    def insert_operation(self) -> SQLOperation:
        self.decode_data()
        insert_clause = self.insert_clause()
        sql = (
            f"INSERT INTO {self.address.fqn} "
            f"({insert_clause.render_lvals()}) VALUES ({insert_clause.render_rvals()}) ON CONFLICT DO NOTHING;"
        )
        parameters = self.bucket.data
        return SQLOperation(sql, parameters)

    def columns_ddl(self) -> t.List[str]:
        """
        Return primary key clause in string format.
        """
        items = []
        columns = self.bucket.control.get("table-def", {}).get("columns", {})
        for column_name, col_meta in columns.items():
            ltype = col_meta.get("type", "TEXT")
            item = f'"{column_name}" {self.resolve_type(ltype)}'
            if column_name in self.primary_keys:
                item += " PRIMARY KEY"
            items.append(item)
        return items

    def insert_clause(self) -> SQLParameterizedSetClause:
        clause = SQLParameterizedSetClause()
        for column, value in self.bucket.data.items():
            clause.add(lval=column, value=value, name=column)
        return clause

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
        for column, value in self.bucket.data.items():
            # Skip primary key columns, they cannot be updated.
            if column in self.primary_keys:
                continue
            clause.add(lval=column, value=value, name=column)
        return clause

    def keys_to_where(self) -> SQLParameterizedWhereClause:
        """
        Produce an SQL WHERE clause based on primary key definition and current record's data.
        """
        if not self.primary_keys:
            raise ValueError("Unable to invoke DML operation without primary key information")
        clause = SQLParameterizedWhereClause()
        for key_name in self.primary_keys:
            key_value = self.bucket.data.get(key_name)
            if key_value is not None:
                clause.add(lval=key_name, value=key_value, name=key_name)
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
        mapping_strategy: t.Dict[TableAddress, ColumnMappingStrategy] = None,
        ignore_ddl: t.Dict[TableAddress, bool] = None,
    ):
        self.primary_keys = primary_keys or PrimaryKeyStore()
        self.column_types = column_types or ColumnTypeMapStore()
        self.mapping_strategy = mapping_strategy or {}
        self.ignore_ddl = ignore_ddl or {}

        # Store caller-provided schema information to restore this state on `DROP TABLE` operations.
        self.primary_keys_caller = copy.deepcopy(self.primary_keys)
        self.column_types_caller = copy.deepcopy(self.column_types)

    def to_sql(self, record: t.Dict[str, t.Any]) -> SQLOperation:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from load|insert|update|delete DMS CDC event record.
        """
        decoder = DMSTranslatorCrateDBRecordFactory.create(event=record, container=self)
        return decoder.to_sql()
