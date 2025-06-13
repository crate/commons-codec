# Copyright (c) 2021-2024, Crate.io Inc.
# Distributed under the terms of the LGPLv3 license, see LICENSE.
# ruff: noqa: S608
import base64
import calendar
import datetime as dt
import logging
import typing as t
from functools import lru_cache
from typing import Iterable

import bson
import dateutil.parser as dateparser
from attrs import define
from bson.json_util import _json_convert, object_hook
from pymongo.cursor import Cursor
from sqlalchemy_cratedb.support import quote_relation_name

from commons_codec.model import SQLOperation

Document = t.Mapping[str, t.Any]
DocumentCollection = t.List[Document]


logger = logging.getLogger(__name__)


@lru_cache()
def all_bson_types() -> t.Tuple[t.Type, ...]:
    _types: t.List[t.Type] = []
    for _typ in bson._ENCODERS:
        if hasattr(_typ, "_type_marker"):
            _types.append(_typ)
    return tuple(_types)


@define
class MongoDBCrateDBConverter:
    """
    Convert MongoDB Extended JSON to representation consumable by CrateDB.

    Extracted from cratedb-toolkit, earlier migr8.
    """

    timestamp_to_epoch: bool = False
    timestamp_to_iso8601: bool = False
    timestamp_use_milliseconds: bool = False
    transformation: t.Any = None

    def decode_documents(self, data: t.Iterable[Document]) -> Iterable[Document]:
        """
        Decode MongoDB Extended JSON, considering CrateDB specifics.
        """
        data = map(self.decode_bson, data)
        data = map(self.decode_value, data)
        # TODO: This is currently untyped. Types are defined in Tikray, see `tikray.model.base`.
        if self.transformation is not None:
            data = self.transformation.apply(data)
        return data

    def decode_document(self, data: Document) -> Document:
        """
        Decode MongoDB Extended JSON, considering CrateDB specifics.
        """
        return self.decode_value(self.decode_bson(data))

    def decode_value(self, value: t.Any) -> t.Any:
        """
        Decode MongoDB Extended JSON.

        - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json-v1/
        - https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/
        """
        if isinstance(value, dict):
            # Decode item in BSON CANONICAL format.
            if len(value) == 1 and next(iter(value)).startswith("$"):
                return self.decode_extended_json(value)

            # Custom adjustments to compensate shape anomalies in source data.
            # TODO: Review if it can be removed or refactored.
            self.apply_special_treatments(value)

            return {k: self.decode_value(v) for (k, v) in value.items()}
        elif isinstance(value, list):
            return [self.decode_value(v) for v in value]

        return value

    @staticmethod
    def decode_bson(item: t.Mapping[str, t.Any]) -> t.Mapping[str, t.Any]:
        """
        Decode data structure including BSON or native Python types to MongoDB Extended JSON format.

        https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/

        Example:

        IN:
        {
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

    def decode_extended_json(self, value: t.Dict[str, t.Any]) -> t.Any:
        """
        Decode MongoDB Extended JSON representation, canonical and legacy variants.
        """

        out: t.Any

        # Special handling for datetime representation in NUMBERLONG format (emulated depth-first).
        type_ = next(iter(value))  # Get key of first item in dictionary.
        if type_ == "$date" and isinstance(value["$date"], dict):
            value = {"$date": int(value["$date"]["$numberLong"])}

        # Invoke BSON decoder.
        try:
            out = object_hook(value)
        except bson.errors.InvalidBSON as ex:
            logger.error(f"Decoding BSON value failed: {ex}. value={value}")
            out = None
            if "Python int too large to convert to C int" in str(ex):
                out = 0

        is_bson = isinstance(out, all_bson_types())

        # Decode BSON types.
        if isinstance(out, bson.Binary) and out.subtype == bson.UUID_SUBTYPE:
            out = out.as_uuid()
        elif isinstance(out, bson.Binary):
            out = base64.b64encode(out).decode()
        elif isinstance(out, bson.Timestamp):
            out = out.as_datetime()

        # Decode Python types.
        if isinstance(out, dt.datetime):
            if self.timestamp_to_epoch:
                out = self.convert_epoch(out)
                if self.timestamp_use_milliseconds:
                    out *= 1000
                return out
            elif self.timestamp_to_iso8601:
                return self.convert_iso8601(out)

        # Wrap up decoded BSON types as strings.
        if is_bson:
            return str(out)

        # Return others converted as-is.
        return out

    @staticmethod
    def convert_epoch(value: t.Any) -> float:
        if isinstance(value, int):
            return value
        elif isinstance(value, dt.datetime):
            datetime = value
        elif isinstance(value, (str, bytes)):
            datetime = dateparser.parse(value)
        else:
            raise ValueError(f"Unable to convert datetime value: {value}")
        return calendar.timegm(datetime.utctimetuple())

    @staticmethod
    def convert_iso8601(value: t.Any) -> str:
        if isinstance(value, str):
            return value
        elif isinstance(value, dt.datetime):
            datetime = value
        elif isinstance(value, bytes):
            return value.decode("utf-8")
        elif isinstance(value, int):
            datetime = dt.datetime.fromtimestamp(value, tz=dt.timezone.utc)
        else:
            raise ValueError(f"Unable to convert datetime value: {value}")
        return datetime.isoformat()

    def apply_special_treatments(self, value: t.Any):
        """
        Apply special treatments to value that can't be described otherwise up until now.
        # Ignore certain items including anomalies that are not resolved, yet.

        TODO: Needs an integration test feeding two records instead of just one.
        """

        if self.transformation is None or self.transformation.treatment is None:
            return value

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

    def __init__(self, table_name: str, converter: t.Union[MongoDBCrateDBConverter, None] = None):
        self.table_name = quote_relation_name(table_name)
        self.converter = converter or MongoDBCrateDBConverter(timestamp_to_epoch=True, timestamp_use_milliseconds=True)

    @property
    def sql_ddl(self):
        """
        Define SQL DDL statement for creating table in CrateDB that stores re-materialized CDC events.
        """
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ({self.ID_COLUMN} TEXT, {self.DATA_COLUMN} OBJECT(DYNAMIC));"
        )


class MongoDBFullLoadTranslator(MongoDBTranslatorBase):
    """
    Translate a MongoDB document into a CrateDB document.
    """

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
        for record in self.converter.decode_documents(data):
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
            document = self.get_full_document(event)
            record = self.converter.decode_document(document)
            sql = f"INSERT INTO {self.table_name} ({self.ID_COLUMN}, {self.DATA_COLUMN}) VALUES (:oid, :record);"
            parameters = {"oid": oid, "record": record}

        # In order to use "full document" representations from "update" events,
        # you need to use `watch(full_document="updateLookup")`.
        # https://www.mongodb.com/docs/manual/changeStreams/#lookup-full-document-for-update-operations
        elif operation_type in ["update", "replace"]:
            document = self.get_full_document(event)
            record = self.converter.decode_document(document)
            where_clause = self.where_clause(event)
            sql = f"UPDATE {self.table_name} SET {self.DATA_COLUMN} = :record WHERE {where_clause};"
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
