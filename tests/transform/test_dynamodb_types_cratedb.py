import unittest
from decimal import Decimal

import pytest

from commons_codec.model import DualRecord
from commons_codec.transform.dynamodb import CrateDBTypeDeserializer, DynamoDBCDCTranslator

pytestmark = pytest.mark.dynamodb


class TestDeserializer(unittest.TestCase):
    def setUp(self):
        self.deserializer = CrateDBTypeDeserializer()

    def test_deserialize_list(self):
        assert self.deserializer.deserialize({"L": [{"N": "1"}, {"S": "foo"}, {"L": [{"N": "1.25"}]}]}) == [
            Decimal("1"),
            "foo",
            [Decimal("1.25")],
        ]


def test_decode_typed_untyped():
    assert DynamoDBCDCTranslator(table_name="foo").decode_record(
        {"foo": {"N": "84.84"}, "bar": {"L": [{"N": "1"}, {"S": "foo"}]}}
    ) == DualRecord(typed={"foo": 84.84}, untyped={"bar": [1.0, "foo"]})
