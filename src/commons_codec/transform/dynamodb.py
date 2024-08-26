# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
import decimal
import logging
import typing as t

import toolz

from commons_codec.model import (
    SQLOperation,
    SQLParameterizedSetClause,
    SQLParameterizedWhereClause,
)
from commons_codec.vendor.boto3.dynamodb.types import DYNAMODB_CONTEXT, TypeDeserializer

logger = logging.getLogger(__name__)

# Inhibit Inexact Exceptions
DYNAMODB_CONTEXT.traps[decimal.Inexact] = False
# Inhibit Rounded Exceptions
DYNAMODB_CONTEXT.traps[decimal.Rounded] = False


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


class DynamoTranslatorBase:
    """
    Translate DynamoDB CDC events into different representations.
    """

    # Define name of the column where CDC's record data will get materialized into.
    DATA_COLUMN = "data"

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = self.quote_table_name(table_name)
        self.deserializer = CrateDBTypeDeserializer()

    @property
    def sql_ddl(self):
        """`
        Define SQL DDL statement for creating table in CrateDB that stores re-materialized CDC events.
        """
        return f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.DATA_COLUMN} OBJECT(DYNAMIC));"

    @staticmethod
    def quote_table_name(name: str):
        """
        Poor man's table quoting.

        TODO: Better use or vendorize canonical table quoting function from CrateDB Toolkit, when applicable.
        """
        if '"' not in name and "." not in name:
            name = f'"{name}"'
        return name

    def decode_record(self, item: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
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
        return toolz.valmap(self.deserializer.deserialize, item)


class DynamoDBFullLoadTranslator(DynamoTranslatorBase):
    def to_sql(self, record: t.Dict[str, t.Any]) -> SQLOperation:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from INSERT|MODIFY|REMOVE CDC event record.
        """
        record = self.decode_record(record)
        sql = f"INSERT INTO {self.table_name} ({self.DATA_COLUMN}) VALUES (:record);"
        return SQLOperation(sql, {"record": record})


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
            record = self.decode_record(event["dynamodb"]["NewImage"])
            sql = f"INSERT INTO {self.table_name} ({self.DATA_COLUMN}) VALUES (:record);"
            parameters = {"record": record}

        elif event_name == "MODIFY":
            new_image = event["dynamodb"]["NewImage"]
            # Drop primary key columns to not update them.
            # Primary key values should be identical (if chosen identical in DynamoDB and CrateDB),
            # but CrateDB does not allow having them in an UPDATE's SET clause.
            for key in event["dynamodb"]["Keys"]:
                del new_image[key]

            record = self.decode_record(event["dynamodb"]["NewImage"])
            set_clause = self.update_clause(record)

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

    def update_clause(self, record: t.Dict[str, t.Any]) -> SQLParameterizedSetClause:
        """
        Serializes an image to a comma-separated list of column/values pairs
        that can be used in the `SET` clause of an `UPDATE` statement.

        IN:
        {'humidity': {'N': '84.84'}, 'temperature': {'N': '55.66'}}

        OUT:
        data['humidity'] = '84.84', data['temperature'] = '55.66'
        """

        clause = SQLParameterizedSetClause()
        for column, value in record.items():
            rval = None
            if isinstance(value, dict):
                rval = f"CAST(:{column} AS OBJECT)"

            elif isinstance(value, list) and value and isinstance(value[0], dict):
                rval = f"CAST(:{column} AS OBJECT[])"

            clause.add(lval=f"{self.DATA_COLUMN}['{column}']", name=column, value=value, rval=rval)
        return clause

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
        keys = self.decode_record(keys)
        clause = SQLParameterizedWhereClause()
        for key_name, key_value in keys.items():
            clause.add(lval=f"{self.DATA_COLUMN}['{key_name}']", name=key_name, value=key_value)
        return clause
