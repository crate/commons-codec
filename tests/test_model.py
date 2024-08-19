import pytest

from commons_codec.model import ColumnType, ColumnTypeMapStore, TableAddress


def test_table_address_success():
    ta = TableAddress(schema="foo", table="bar")
    assert ta.fqn == '"foo"."bar"'


def test_table_address_failure():
    ta = TableAddress(schema=None, table="bar")
    with pytest.raises(ValueError) as ex:
        _ = ta.fqn
        assert ex.match("adcdc")


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


def test_column_type_map_store_unserialize_empty():
    assert ColumnTypeMapStore.from_json("") is None
    assert ColumnTypeMapStore.from_json(None) is None
    assert ColumnTypeMapStore.from_dict(None) is None
