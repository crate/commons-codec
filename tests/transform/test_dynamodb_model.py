import pytest

from commons_codec.transform.dynamodb_model import PrimaryKeySchema


def test_primary_key_schema_from_table_success():
    class SurrogateTable:
        attribute_definitions = [
            {"AttributeName": "Id", "AttributeType": "N"},
        ]
        key_schema = [
            {"AttributeName": "Id", "KeyType": "HASH"},
        ]

    pks = PrimaryKeySchema.from_table(SurrogateTable())
    assert pks == PrimaryKeySchema().add("Id", "N")
    assert pks.column_names() == ['"Id"']


def test_primary_key_schema_from_table_unknown_type():
    class SurrogateTable:
        attribute_definitions = [
            {"AttributeName": "Id", "AttributeType": "F"},
        ]
        key_schema = [
            {"AttributeName": "Id", "KeyType": "HASH"},
        ]

    with pytest.raises(KeyError) as ex:
        PrimaryKeySchema.from_table(SurrogateTable())
    assert ex.match("Mapping DynamoDB type failed: name=Id, type=F")
