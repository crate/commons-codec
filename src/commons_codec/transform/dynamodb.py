# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import decimal
import logging
import typing as t

import toolz
from sqlalchemy_cratedb.support import quote_relation_name

from commons_codec.model import (
    SQLOperation,
    UniversalRecord,
)
from commons_codec.transform.dynamodb_model import PrimaryKeySchema
from commons_codec.util.data import TaggableList
from commons_codec.vendor.boto3.dynamodb.types import DYNAMODB_CONTEXT, TypeDeserializer

logger = logging.getLogger(__name__)

# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[decimal.Inexact] = False
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = False

RecordType = t.Dict[str, t.Any]


class CrateDBTypeDeserializer(TypeDeserializer):
    def _deserialize_n(self, value):
        return float(super()._deserialize_n(value))

    def _deserialize_b(self, value):
        return value

    def _deserialize_ns(self, value):
        return list(super()._deserialize_ns(value))

    def _deserialize_ss(self, value):
        return list(super()._deserialize_ss(value))

    def _deserialize_bs(self, value):
        return list(super()._deserialize_bs(value))

    def _deserialize_l(self, value):
        """
        CrateDB can't store varied lists in an OBJECT(DYNAMIC) column, so set the
        stage to break them apart in order to store them in an OBJECT(IGNORED) column.

        https://github.com/crate/commons-codec/issues/28
        """

        # Deserialize list as-is.
        result = TaggableList([self.deserialize(v) for v in value])
        result.set_tag("varied", False)

        # If it's not an empty list, check if inner types are varying.
        # If so, tag the result list accordingly.
        # It doesn't work on the result list itself, but on the DynamoDB
        # data structure instead, comparing the single/dual-letter type
        # identifiers.
        if value:
            dynamodb_type_first = list(value[0].keys())[0]
            for v in value:
                dynamodb_type_current = list(v.keys())[0]
                if dynamodb_type_current != dynamodb_type_first:
                    result.set_tag("varied", True)
                    break
        return result


class DynamoTranslatorBase:
    """
    Translate DynamoDB records into a different representation.
    """

    # Define name of the column where KeySchema DynamoDB fields will get materialized into.
    # This column uses the `OBJECT(DYNAMIC)` data type.
    PK_COLUMN = "pk"

    # Define name of the column where typed DynamoDB fields will get materialized into.
    # This column uses the `OBJECT(DYNAMIC)` data type.
    TYPED_COLUMN = "data"

    # Define name of the column where untyped DynamoDB fields will get materialized into.
    # This column uses the `OBJECT(IGNORED)` data type.
    UNTYPED_COLUMN = "aux"

    def __init__(self, table_name: str, primary_key_schema: PrimaryKeySchema = None):
        super().__init__()
        self.table_name = quote_relation_name(table_name)
        self.primary_key_schema = primary_key_schema
        self.deserializer = CrateDBTypeDeserializer()

    @property
    def sql_ddl(self):
        """`
        Define SQL DDL statement for creating table in CrateDB that stores re-materialized CDC events.
        """
        if self.primary_key_schema is None:
            raise IOError("Unable to generate SQL DDL without key schema information")
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
            f"{self.PK_COLUMN} OBJECT(STRICT) AS ({', '.join(self.primary_key_schema.to_sql_ddl_clauses())}), "
            f"{self.TYPED_COLUMN} OBJECT(DYNAMIC), "
            f"{self.UNTYPED_COLUMN} OBJECT(IGNORED));"
        )

    def decode_record(self, item: t.Dict[str, t.Any], key_names: t.Union[t.List[str], None] = None) -> UniversalRecord:
        """
        Deserialize DynamoDB JSON record into vanilla Python.

        Example:
        {
            "humidity": {"N": "84.84"},
            "temperature": {"N": "42.42"},
            "device": {"S": "qux"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
        }

        A complete list of DynamoDB data type descriptors:

        S – String
        N – Number
        B – Binary
        BOOL – Boolean
        NULL – Null
        M – Map
        L – List
        SS – String Set
        NS – Number Set
        BS – Binary Set

        -- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypeDescriptors
        """
        record = toolz.valmap(self.deserializer.deserialize, item)
        return UniversalRecord.from_record(
            record, key_names or (self.primary_key_schema and self.primary_key_schema.keys() or None)
        )


class DynamoDBFullLoadTranslator(DynamoTranslatorBase):
    def to_sql(self, data: t.Union[RecordType, t.List[RecordType]]) -> SQLOperation:
        """
        Produce INSERT SQL operations (SQL statement and parameters) from DynamoDB record(s).
        """
        sql = (
            f"INSERT INTO {self.table_name} ("
            f"{self.PK_COLUMN}, "
            f"{self.TYPED_COLUMN}, "
            f"{self.UNTYPED_COLUMN}"
            f") VALUES ("
            f":pk, "
            f":typed, "
            f":untyped);"
        )
        if not isinstance(data, list):
            data = [data]
        parameters = [self.decode_record(record).to_dict() for record in data]
        return SQLOperation(sql, parameters)


class DynamoDBCDCTranslator(DynamoTranslatorBase):
    """
    Translate DynamoDB CDC events into CrateDB SQL statements that materialize them again.

    The SQL DDL schema for CrateDB:
    CREATE TABLE <tablename> (data OBJECT(DYNAMIC));

    Blueprint:
    https://www.singlestore.com/blog/cdc-data-from-dynamodb-to-singlestore-using-dynamodb-streams/
    """

    def to_sql(self, event: t.Dict[str, t.Any]) -> SQLOperation:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from INSERT|MODIFY|REMOVE CDC event record.
        """
        event_source = event.get("eventSource")
        event_name = event.get("eventName")

        if event_source != "aws:dynamodb":
            raise ValueError(f"Unknown eventSource: {event_source}")

        if event_name == "INSERT":
            record = self.decode_event(event["dynamodb"])
            sql = (
                f"INSERT INTO {self.table_name} ("
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

        elif event_name == "MODIFY":
            record = self.decode_event(event["dynamodb"])
            sql = (
                f"UPDATE {self.table_name} "
                f"SET {self.TYPED_COLUMN}=:typed, {self.UNTYPED_COLUMN}=:untyped "
                f"WHERE {self.PK_COLUMN}=:pk;"
            )
            parameters = record.to_dict()

        elif event_name == "REMOVE":
            record = self.decode_event(event["dynamodb"])
            sql = f"DELETE FROM {self.table_name} WHERE {self.PK_COLUMN}=:pk;"
            parameters = record.to_dict()

        else:
            raise ValueError(f"Unknown CDC event name: {event_name}")

        return SQLOperation(sql, parameters)

    def decode_event(self, event: t.Dict[str, t.Any]) -> UniversalRecord:
        # That's for INSERT+MODIFY.
        if "NewImage" in event:
            return self.decode_record(event["NewImage"], event["Keys"].keys())

        # That's for REMOVE.
        else:
            return self.decode_record(event["Keys"], event["Keys"].keys())
