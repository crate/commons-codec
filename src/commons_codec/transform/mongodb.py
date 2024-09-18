# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
# ruff: noqa: S608
import base64
import calendar
import logging
import typing as t
from typing import Any, Iterable
from uuid import UUID

import dateutil.parser as dateparser
from attr import Factory
from attrs import define
from bson.json_util import _json_convert
from pymongo.cursor import Cursor
from sqlalchemy_cratedb.support import quote_relation_name

from commons_codec.model import SQLOperation
from zyp.model.collection import CollectionTransformation

logger = logging.getLogger(__name__)


Document = t.Mapping[str, t.Any]
DocumentCollection = t.List[Document]


def date_converter(value):
    if isinstance(value, int):
        return value
    dt = dateparser.parse(value)
    return calendar.timegm(dt.utctimetuple()) * 1000


def timestamp_converter(value):
    if len(str(value)) <= 10:
        return value * 1000
    return value


type_converter = {
    "date": date_converter,
    "timestamp": timestamp_converter,
    "undefined": lambda x: None,
}


@define
class MongoDBCrateDBConverter:
    """
    Convert MongoDB Extended JSON to representation consumable by CrateDB.

    Extracted from cratedb-toolkit, earlier migr8.
    """

    transformation: CollectionTransformation = Factory(CollectionTransformation)

    def decode_documents(self, data: t.List[Document]) -> Iterable[dict[str, Any]]:
        """
        Decode MongoDB Extended JSON, considering CrateDB specifics.
        """
        return self.transformation.apply(map(self.extract_value, data))

    def decode_document(self, data: Document) -> Document:
        """
        Decode MongoDB Extended JSON, considering CrateDB specifics.
        """
        return self.extract_value(data)

    def extract_value(self, value: t.Any, parent_type: t.Optional[str] = None) -> t.Any:
        """
        Decode MongoDB Extended JSON.

        - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json-v1/
        - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
        """
        if isinstance(value, dict):
            # Custom adjustments to compensate shape anomalies in source data.
            self.apply_special_treatments(value)
            if len(value) == 1:
                if "$binary" in value and value["$binary"]["subType"] in ["03", "04"]:
                    decoded = str(UUID(bytes=base64.b64decode(value["$binary"]["base64"])))
                    return self.extract_value(decoded, parent_type)
                for k, v in value.items():
                    if k.startswith("$"):
                        return self.extract_value(v, k.lstrip("$"))
            return {k.lstrip("$"): self.extract_value(v, parent_type) for (k, v) in value.items()}
        if isinstance(value, list):
            return [self.extract_value(v, parent_type) for v in value]
        if parent_type:
            converter = type_converter.get(parent_type)
            if converter:
                return converter(value)
        return value

    def apply_special_treatments(self, value: t.Any):
        """
        Apply special treatments to value that can't be described otherwise up until now.
        # Ignore certain items including anomalies that are not resolved, yet.

        TODO: Needs an integration test feeding two records instead of just one.
        """

        if self.transformation is None or self.transformation.treatment is None:
            return None

        return self.transformation.treatment.apply(value)


class MongoDBTranslatorBase:
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

    # Define name of the column where MongoDB's OID for a document will be stored.
    ID_COLUMN = "oid"

    # Define name of the column where CDC's record data will get materialized into.
    DATA_COLUMN = "data"

    def __init__(self, table_name: str):
        super().__init__()
        self.table_name = quote_relation_name(table_name)

    @property
    def sql_ddl(self):
        """
        Define SQL DDL statement for creating table in CrateDB that stores re-materialized CDC events.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.ID_COLUMN} TEXT, {self.DATA_COLUMN} OBJECT(DYNAMIC));"
        )

    @staticmethod
    def decode_bson(item: t.Mapping[str, t.Any]) -> t.Mapping[str, t.Any]:
        """
        Convert MongoDB Extended JSON to vanilla Python dictionary.

        https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/

        Example:
        {
          "_id": ObjectId("669683c2b0750b2c84893f3e"),
          "id": "5F9E",
          "data": {"temperature": 42.42, "humidity": 84.84},
          "meta": {"timestamp": datetime.datetime(2024, 7, 11, 23, 17, 42), "device": "foo"},
        }

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
        return _json_convert(item)


class MongoDBFullLoadTranslator(MongoDBTranslatorBase):
    """
    Translate a MongoDB document into a CrateDB document.
    """

    def __init__(self, table_name: str, converter: MongoDBCrateDBConverter):
        super().__init__(table_name=table_name)
        self.converter = converter

    @staticmethod
    def get_document_key(record: t.Mapping[str, t.Any]) -> str:
        """
        Return value of document key (MongoDB document OID).

        "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")}
        """
        return record["_id"]

    def to_sql(self, data: t.Union[Document, t.List[Document]]) -> SQLOperation:
        """
        Produce CrateDB SQL INSERT batch operation from multiple MongoDB documents.
        """
        if not isinstance(data, Cursor) and not isinstance(data, list):
            data = [data]

        # Define SQL INSERT statement.
        sql = f"INSERT INTO {self.table_name} ({self.ID_COLUMN}, {self.DATA_COLUMN}) VALUES (:oid, :record);"

        # Converge multiple MongoDB documents into SQL parameters for `executemany` operation.
        parameters: t.List[Document] = []
        for document in data:
            record = self.converter.decode_document(self.decode_bson(document))
            oid: str = self.get_document_key(record)
            parameters.append({"oid": oid, "record": record})

        return SQLOperation(sql, parameters)


class MongoDBCDCTranslator(MongoDBTranslatorBase):
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

    def to_sql(self, event: t.Dict[str, t.Any]) -> t.Union[SQLOperation, None]:
        """
        Produce INSERT|UPDATE|DELETE SQL statement from insert|update|replace|delete CDC event record.
        """

        if "operationType" in event and event["operationType"]:
            operation_type: str = str(event["operationType"])
        else:
            raise ValueError(f"Operation Type missing or empty: {event}")

        if operation_type == "insert":
            oid: str = self.get_document_key(event)
            record = self.decode_bson(self.get_full_document(event))
            sql = f"INSERT INTO {self.table_name} " f"({self.ID_COLUMN}, {self.DATA_COLUMN}) " "VALUES (:oid, :record);"
            parameters = {"oid": oid, "record": record}

        # In order to use "full document" representations from "update" events,
        # you need to use `watch(full_document="updateLookup")`.
        # https://www.mongodb.com/docs/manual/changeStreams/#lookup-full-document-for-update-operations
        elif operation_type in ["update", "replace"]:
            record = self.decode_bson(self.get_full_document(event))
            where_clause = self.where_clause(event)
            sql = f"UPDATE {self.table_name} " f"SET {self.DATA_COLUMN} = :record " f"WHERE {where_clause};"
            parameters = {"record": record}

        elif operation_type == "delete":
            where_clause = self.where_clause(event)
            sql = f"DELETE FROM {self.table_name} WHERE {where_clause};"
            parameters = None

        # TODO: Enable applying the "drop" operation conditionally when enabled.
        elif operation_type == "drop":
            logger.info("Received 'drop' operation, but skipping to apply 'DROP TABLE'")
            return None

        elif operation_type == "invalidate":
            logger.info("Ignoring 'invalidate' CDC operation")
            return None

        else:
            raise ValueError(f"Unknown CDC operation type: {operation_type}")

        return SQLOperation(sql, parameters)

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
