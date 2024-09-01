# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import decimal
import logging
import typing as t

import toolz
from sqlalchemy_cratedb.support import quote_relation_name

from commons_codec.model import (
    DualRecord,
    SQLOperation,
    SQLParameterizedSetClause,
    SQLParameterizedWhereClause,
)
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

    # Define name of the column where typed DynamoDB fields will get materialized into.
    # This column uses the `OBJECT(DYNAMIC)` data type.
    TYPED_COLUMN = "data"

    # Define name of the column where untyped DynamoDB fields will get materialized into.
    # This column uses the `OBJECT(IGNORED)` data type.
    UNTYPED_COLUMN = "aux"

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = quote_relation_name(table_name)
        self.deserializer = CrateDBTypeDeserializer()

    @property
    def sql_ddl(self):
        """`
        Define SQL DDL statement for creating table in CrateDB that stores re-materialized CDC events.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} "
            f"({self.TYPED_COLUMN} OBJECT(DYNAMIC), {self.UNTYPED_COLUMN} OBJECT(IGNORED));"
        )

    def decode_record(self, item: t.Dict[str, t.Any]) -> DualRecord:
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
        untyped = {}
        for key, value in record.items():
            if isinstance(value, TaggableList) and value.get_tag("varied", False):
                untyped[key] = value
        record = toolz.dissoc(record, *untyped.keys())
        return DualRecord(typed=record, untyped=untyped)


class DynamoDBFullLoadTranslator(DynamoTranslatorBase):
    def to_sql(self, data: t.Union[RecordType, t.List[RecordType]]) -> SQLOperation:
        """
        Produce INSERT SQL operations (SQL statement and parameters) from DynamoDB record(s).
        """
        sql = f"INSERT INTO {self.table_name} ({self.TYPED_COLUMN}, {self.UNTYPED_COLUMN}) VALUES (:typed, :untyped);"
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
            dual_record = self.decode_record(event["dynamodb"]["NewImage"])
            sql = (
                f"INSERT INTO {self.table_name} ({self.TYPED_COLUMN}, {self.UNTYPED_COLUMN}) VALUES (:typed, :untyped);"
            )
            parameters = {"typed": dual_record.typed, "untyped": dual_record.untyped}

        elif event_name == "MODIFY":
            new_image = event["dynamodb"]["NewImage"]
            # Drop primary key columns to not update them.
            # Primary key values should be identical (if chosen identical in DynamoDB and CrateDB),
            # but CrateDB does not allow having them in an UPDATE's SET clause.
            for key in event["dynamodb"]["Keys"]:
                del new_image[key]

            dual_record = self.decode_record(event["dynamodb"]["NewImage"])
            set_clause = self.update_clause(dual_record)

            where_clause = self.keys_to_where(event["dynamodb"]["Keys"])
            sql = f"UPDATE {self.table_name} SET {set_clause.to_sql()} WHERE {where_clause.to_sql()};"
            parameters = set_clause.values  # noqa: PD011
            parameters.update(where_clause.values)

        elif event_name == "REMOVE":
            where_clause = self.keys_to_where(event["dynamodb"]["Keys"])
            sql = f"DELETE FROM {self.table_name} WHERE {where_clause.to_sql()};"
            parameters = where_clause.values  # noqa: PD011

        else:
            raise ValueError(f"Unknown CDC event name: {event_name}")

        return SQLOperation(sql, parameters)

    def update_clause(self, dual_record: DualRecord) -> SQLParameterizedSetClause:
        """
        Serializes an image to a comma-separated list of column/values pairs
        that can be used in the `SET` clause of an `UPDATE` statement.

        IN:
        {'humidity': {'N': '84.84'}, 'temperature': {'N': '55.66'}}

        OUT:
        data['humidity'] = '84.84', data['temperature'] = '55.66'
        """

        clause = SQLParameterizedSetClause()
        self.record_to_set_clause(dual_record.typed, self.TYPED_COLUMN, clause)
        self.record_to_set_clause(dual_record.untyped, self.UNTYPED_COLUMN, clause)
        return clause

    @staticmethod
    def record_to_set_clause(record: t.Dict[str, t.Any], container_column: str, clause: SQLParameterizedSetClause):
        for column, value in record.items():
            rval = None
            if isinstance(value, dict):
                rval = f"CAST(:{column} AS OBJECT)"

            elif isinstance(value, list) and value and isinstance(value[0], dict):
                rval = f"CAST(:{column} AS OBJECT[])"

            clause.add(lval=f"{container_column}['{column}']", name=column, value=value, rval=rval)

    def keys_to_where(self, keys: t.Dict[str, t.Dict[str, str]]) -> SQLParameterizedWhereClause:
        """
        Serialize CDC event's "Keys" representation to an SQL `WHERE` clause in CrateDB SQL syntax.

        IN (top-level stripped):
        "Keys": {
            "device": {"S": "foo"},
            "timestamp": {"S": "2024-07-12T01:17:42"},
        }

        OUT:
        WHERE data['device'] = 'foo' AND data['timestamp'] = '2024-07-12T01:17:42'
        """
        dual_record = self.decode_record(keys)
        clause = SQLParameterizedWhereClause()
        for key_name, key_value in dual_record.typed.items():
            clause.add(lval=f"{self.TYPED_COLUMN}['{key_name}']", name=key_name, value=key_value)
        return clause
