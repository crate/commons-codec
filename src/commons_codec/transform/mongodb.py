# Copyright (c) 2023-2024, The Kotori Developers and contributors.
# Distributed under the terms of the LGPLv3 license, see LICENSE.

# ruff: noqa: S608 FIXME: Possible SQL injection vector through string-based query construction

import logging
import typing as t

import simplejson as json
from bson.json_util import _json_convert

logger = logging.getLogger(__name__)


class MongoDBCDCTranslatorBase:
    """
    Translate MongoDB CDC events into different representations.

    Change streams allow applications to access real-time data changes without the prior
    complexity and risk of manually tailing the oplog. Applications can use change streams
    to subscribe to all data changes on a single collection, a database, or an entire
    deployment, and immediately react to them.

    Because change streams use the aggregation framework, applications can also filter
    for specific changes or transform the notifications at will.

    - https://www.mongodb.com/docs/manual/changeStreams/
    - https://www.mongodb.com/developer/languages/python/python-change-streams/
    """

    def deserialize_item(self, item: t.Dict[str, t.Dict[str, str]]) -> t.Dict[str, str]:
        """
        Deserialize MongoDB type-enriched nested JSON snippet into vanilla Python.

        Example:
        {
          "_id": ObjectId("669683c2b0750b2c84893f3e"),
          "id": "5F9E",
          "data": {"temperature": 42.42, "humidity": 84.84},
          "meta": {"timestamp": datetime.datetime(2024, 7, 11, 23, 17, 42), "device": "foo"},
        }
        """
        return _json_convert(item)


class MongoDBCDCTranslatorCrateDB(MongoDBCDCTranslatorBase):
    """
    Translate MongoDB CDC events into CrateDB SQL statements that materialize them again.

    Please note that change streams are only available for replica sets and sharded clusters.

    Accepted events: insert, update, replace, delete
    Ignored events: drop, invalidate

    The current implementation uses the `fullDocument` representation to update records
    in the sink database table. In order to receive them on `update` events as well, you
    need to subscribe to change events using `watch(full_document="updateLookup")`.

    The MongoDB documentation has a few remarks about the caveats of this approach:

    > Updates with the `fullDocument` Option: The `fullDocument` option for Update Operations
    > does not guarantee the returned document does not include further changes. In contrast
    > to the document deltas that are guaranteed to be sent in order with update notifications,
    > there is no guarantee that the `fullDocument` returned represents the document as it was
    > exactly after the operation.
    >
    > `updateLookup` will poll the current version of the document. If changes happen quickly
    > it is possible that the document was changed before the updateLookup finished. This means
    > that the `fullDocument` might not represent the document at the time of the event thus
    > potentially giving the impression events took place in a different order.
    >
    > -- https://www.mongodb.com/developer/languages/python/python-change-streams/

    The SQL DDL schema for CrateDB:
    CREATE TABLE <tablename> (oid TEXT, data OBJECT(DYNAMIC));
    """

    # Define name of the column where MongoDB's OID for a document will be stored.
    ID_COLUMN = "oid"

    # Define name of the column where CDC's record data will get materialized into.
    DATA_COLUMN = "data"

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = self.quote_table_name(table_name)

    @property
    def sql_ddl(self):
        """
        Define SQL DDL statement for creating table in CrateDB that stores re-materialized CDC events.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.ID_COLUMN} TEXT, {self.DATA_COLUMN} OBJECT(DYNAMIC));"
        )

    def to_sql(self, record: t.Dict[str, t.Any]) -> str:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from insert|update|replace|delete CDC event record.
        """

        if "operationType" in record and record["operationType"]:
            operation_type: str = str(record["operationType"])
        else:
            raise ValueError(f"Operation Type missing or empty: {record}")

        if operation_type == "insert":
            oid: str = self.get_document_key(record)
            full_document = self.get_full_document(record)
            values_clause = self.full_document_to_values(full_document)
            sql = (
                f"INSERT INTO {self.table_name} "
                f"({self.ID_COLUMN}, {self.DATA_COLUMN}) "
                f"VALUES ('{oid}', '{values_clause}');"
            )

        # In order to use "full document" representations from "update" events,
        # you need to use `watch(full_document="updateLookup")`.
        # https://www.mongodb.com/docs/manual/changeStreams/#lookup-full-document-for-update-operations
        elif operation_type in ["update", "replace"]:
            full_document = self.get_full_document(record)
            values_clause = self.full_document_to_values(full_document)
            where_clause = self.where_clause(record)
            sql = f"UPDATE {self.table_name} SET {self.DATA_COLUMN} = '{values_clause}' WHERE {where_clause};"

        elif operation_type == "delete":
            where_clause = self.where_clause(record)
            sql = f"DELETE FROM {self.table_name} WHERE {where_clause};"

        # TODO: Enable applying the "drop" operation conditionally when enabled.
        elif operation_type == "drop":
            logger.info("Received 'drop' operation, but skipping to apply 'DROP TABLE'")
            sql = ""

        elif operation_type == "invalidate":
            logger.info("Ignoring 'invalidate' CDC operation")
            sql = ""

        else:
            raise ValueError(f"Unknown CDC operation type: {operation_type}")

        return sql

    @staticmethod
    def get_document_key(record: t.Dict[str, t.Any]) -> str:
        """
        Return value of document key (MongoDB document OID) from CDC record.

        "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")}
        """
        return str(record.get("documentKey", {}).get("_id"))

    @staticmethod
    def get_full_document(record: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        """
        return `fullDocument` representation from record.
        """
        return t.cast(dict, record.get("fullDocument"))

    def full_document_to_values(self, document: t.Dict[str, t.Any]) -> str:
        """
        Serialize CDC event's "fullDocument" representation to a `VALUES` clause in CrateDB SQL syntax.

        IN (top-level stripped):
        "fullDocument": {
            "_id": ObjectId("669683c2b0750b2c84893f3e"),
            "id": "5F9E",
            "data": {"temperature": 42.42, "humidity": 84.84},
            "meta": {"timestamp": datetime.datetime(2024, 7, 11, 23, 17, 42), "device": "foo"},
        }

        OUT:
        {"_id": {"$oid": "669683c2b0750b2c84893f3e"},
         "id": "5F9E",
         "data": {"temperature": 42.42, "humidity": 84.84},
         "meta": {"timestamp": {"$date": "2024-07-11T23:17:42Z"}, "device": "foo"},
        }
        """
        return json.dumps(self.deserialize_item(document))

    def where_clause(self, record: t.Dict[str, t.Any]) -> str:
        """
        When converging an oplog of a MongoDB collection, the primary key is always the MongoDB document OID.

        IN (top-level stripped):
        "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")}

        OUT:
        WHERE oid = '669683c2b0750b2c84893f3e'
        """
        oid = self.get_document_key(record)
        return f"oid = '{oid}'"

    @staticmethod
    def quote_table_name(name: str):
        """
        Poor man's table quoting.

        TODO: Better use or vendorize canonical table quoting function from CrateDB Toolkit, when applicable.
        """
        if '"' not in name:
            name = f'"{name}"'
        return name
