# ruff: noqa: E402, E501
import pytest

pytestmark = pytest.mark.mongodb

import datetime

from bson import ObjectId, Timestamp

from commons_codec.model import SQLOperation
from commons_codec.transform.mongodb import MongoDBCDCTranslator

MSG_OPERATION_UNKNOWN = {
    "operationType": "foobar",
}
MSG_OPERATION_MISSING = {}
MSG_OPERATION_EMPTY = {
    "operationType": "",
}

MSG_INSERT = {
    "_id": {
        "_data": "82669683C2000000022B042C0100296E5A1004413F85D5B4CF4680AA4D17641E9DF22D463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F69640064669683C2B0750B2C84893F3E000004"
    },
    "operationType": "insert",
    "clusterTime": Timestamp(1721140162, 2),
    "wallTime": datetime.datetime(2024, 7, 16, 14, 29, 22, 907000),
    "fullDocument": {
        "_id": ObjectId("669683c2b0750b2c84893f3e"),
        "id": "5F9E",
        "data": {"temperature": 42.42, "humidity": 84.84},
        "meta": {"timestamp": datetime.datetime(2024, 7, 11, 23, 17, 42), "device": "foo"},
    },
    "ns": {"db": "testdrive", "coll": "data"},
    "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")},
}
MSG_UPDATE = {
    "_id": {
        "_data": "82669683C2000000032B042C0100296E5A1004413F85D5B4CF4680AA4D17641E9DF22D463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B65790046645F69640064669683C2B0750B2C84893F3E000004"
    },
    "operationType": "update",
    "clusterTime": Timestamp(1721140162, 3),
    "wallTime": datetime.datetime(2024, 7, 16, 14, 29, 22, 910000),
    "fullDocument": {
        "_id": ObjectId("669683c2b0750b2c84893f3e"),
        "id": "5F9E",
        "data": {"temperature": 42.5},
        "meta": {"timestamp": datetime.datetime(2024, 7, 11, 23, 17, 42), "device": "foo"},
    },
    "ns": {"db": "testdrive", "coll": "data"},
    "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")},
    "updateDescription": {"updatedFields": {"data": {"temperature": 42.5}}, "removedFields": [], "truncatedArrays": []},
}
MSG_REPLACE = {
    "_id": {
        "_data": "82669683C2000000042B042C0100296E5A1004413F85D5B4CF4680AA4D17641E9DF22D463C6F7065726174696F6E54797065003C7265706C6163650046646F63756D656E744B65790046645F69640064669683C2B0750B2C84893F3E000004"
    },
    "operationType": "replace",
    "clusterTime": Timestamp(1721140162, 4),
    "wallTime": datetime.datetime(2024, 7, 16, 14, 29, 22, 911000),
    "fullDocument": {"_id": ObjectId("669683c2b0750b2c84893f3e"), "tags": ["deleted"]},
    "ns": {"db": "testdrive", "coll": "data"},
    "documentKey": {"_id": ObjectId("669683c2b0750b2c84893f3e")},
}
MSG_DELETE = {
    "_id": {
        "_data": "82669693C5000000032B042C0100296E5A10043D9AA2FA889C45049D2CDE4175242B7E463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B65790046645F69640064669693C5002EF91EA9C7A562000004"
    },
    "operationType": "delete",
    "clusterTime": Timestamp(1721144261, 3),
    "wallTime": datetime.datetime(2024, 7, 16, 15, 37, 41, 831000),
    "ns": {"db": "testdrive", "coll": "data"},
    "documentKey": {"_id": ObjectId("669693c5002ef91ea9c7a562")},
}
MSG_DROP = {
    "_id": {
        "_data": "82669683C2000000052B042C0100296E5A1004413F85D5B4CF4680AA4D17641E9DF22D463C6F7065726174696F6E54797065003C64726F70000004"
    },
    "operationType": "drop",
    "clusterTime": Timestamp(1721140162, 5),
    "wallTime": datetime.datetime(2024, 7, 16, 14, 29, 22, 914000),
    "ns": {"db": "testdrive", "coll": "data"},
}

MSG_INVALIDATE = {
    "_id": {
        "_data": "82669683C2000000052B042C0100296F5A1004413F85D5B4CF4680AA4D17641E9DF22D463C6F7065726174696F6E54797065003C64726F70000004"
    },
    "operationType": "invalidate",
    "clusterTime": Timestamp(1721140162, 5),
    "wallTime": datetime.datetime(2024, 7, 16, 14, 29, 22, 914000),
}


def test_decode_cdc_sql_ddl():
    assert (
        MongoDBCDCTranslator(table_name="foo").sql_ddl
        == "CREATE TABLE IF NOT EXISTS foo (oid TEXT, data OBJECT(DYNAMIC));"
    )


def test_decode_cdc_unknown_event():
    with pytest.raises(ValueError) as ex:
        MongoDBCDCTranslator(table_name="foo").to_sql(MSG_OPERATION_UNKNOWN)
    assert ex.match("Unknown CDC operation type: foobar")


def test_decode_cdc_optype_missing():
    with pytest.raises(ValueError) as ex:
        MongoDBCDCTranslator(table_name="foo").to_sql(MSG_OPERATION_MISSING)
    assert ex.match("Operation Type missing or empty: {}")


def test_decode_cdc_optype_empty():
    with pytest.raises(ValueError) as ex:
        MongoDBCDCTranslator(table_name="foo").to_sql(MSG_OPERATION_EMPTY)
    assert ex.match("Operation Type missing or empty: {'operationType': ''}")


def test_decode_cdc_insert():
    assert MongoDBCDCTranslator(table_name="foo").to_sql(MSG_INSERT) == SQLOperation(
        statement="INSERT INTO foo (oid, data) VALUES (:oid, :record);",
        parameters={
            "oid": "669683c2b0750b2c84893f3e",
            "record": {
                "_id": "669683c2b0750b2c84893f3e",
                "id": "5F9E",
                "data": {"temperature": 42.42, "humidity": 84.84},
                "meta": {"timestamp": 1720739862000, "device": "foo"},
            },
        },
    )


def test_decode_cdc_update():
    assert MongoDBCDCTranslator(table_name="foo").to_sql(MSG_UPDATE) == SQLOperation(
        statement="UPDATE foo SET data = :record WHERE oid = '669683c2b0750b2c84893f3e';",
        parameters={
            "record": {
                "_id": "669683c2b0750b2c84893f3e",
                "id": "5F9E",
                "data": {"temperature": 42.5},
                "meta": {"timestamp": 1720739862000, "device": "foo"},
            }
        },
    )


def test_decode_cdc_replace():
    assert MongoDBCDCTranslator(table_name="foo").to_sql(MSG_REPLACE) == SQLOperation(
        statement="UPDATE foo SET data = :record WHERE oid = '669683c2b0750b2c84893f3e';",
        parameters={"record": {"_id": "669683c2b0750b2c84893f3e", "tags": ["deleted"]}},
    )


def test_decode_cdc_delete():
    assert MongoDBCDCTranslator(table_name="foo").to_sql(MSG_DELETE) == SQLOperation(
        statement="DELETE FROM foo WHERE oid = '669693c5002ef91ea9c7a562';", parameters=None
    )


def test_decode_cdc_drop():
    assert MongoDBCDCTranslator(table_name="foo").to_sql(MSG_DROP) is None


def test_decode_cdc_invalidate():
    assert MongoDBCDCTranslator(table_name="foo").to_sql(MSG_INVALIDATE) is None
