import pytest

from commons_codec.model import ColumnType, ColumnTypeMapStore, TableAddress


def test_table_address_basic():
    ta = TableAddress(schema="foo", table="bar")
    assert ta.fqn == "foo.bar"


def test_table_address_quoting():
    ta = TableAddress(schema="select", table="from")
    assert ta.fqn == '"select"."from"'


def test_table_address_failure():
    ta = TableAddress(schema=None, table="bar")
    with pytest.raises(ValueError) as ex:
        _ = ta.fqn
    assert ex.match("Unable to compute a full-qualified table name without schema name")


def test_column_type_map_store_serialize():
    column_types = ColumnTypeMapStore().add(
        table=TableAddress(schema="public", table="foo"),
        column="attributes",
        type_=ColumnType.MAP,
    )
    assert column_types.to_dict() == {"public:foo:attributes": "map"}
    assert column_types.to_json() == '{"public:foo:attributes": "map"}'


def test_column_type_map_store_unserialize_data():
    assert ColumnTypeMapStore.from_json('{"public:foo:attributes": "map"}') == ColumnTypeMapStore(
        {TableAddress(schema="public", table="foo"): {"attributes": ColumnType.MAP}}
    )


def test_column_type_object_store_serialize():
    column_types = ColumnTypeMapStore().add(
        table=TableAddress(schema="public", table="foo"),
        column="attributes",
        type_=ColumnType.OBJECT,
    )
    assert column_types.to_dict() == {"public:foo:attributes": "object"}
    assert column_types.to_json() == '{"public:foo:attributes": "object"}'


def test_column_type_object_store_unserialize_data():
    assert ColumnTypeMapStore.from_json('{"public:foo:attributes": "object"}') == ColumnTypeMapStore(
        {TableAddress(schema="public", table="foo"): {"attributes": ColumnType.OBJECT}}
    )


def test_column_type_map_store_unserialize_empty():
    assert ColumnTypeMapStore.from_json("") is None
    assert ColumnTypeMapStore.from_json(None) is None
    assert ColumnTypeMapStore.from_dict(None) is None
