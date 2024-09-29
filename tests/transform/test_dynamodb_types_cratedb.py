import unittest
from decimal import Decimal

import pytest

from commons_codec.model import UniversalRecord
from commons_codec.transform.dynamodb import CrateDBTypeDeserializer

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


def test_decode_typed_untyped(dynamodb_cdc_translator_foo):
    assert dynamodb_cdc_translator_foo.decode_record(
        {"foo": {"N": "84.84"}, "bar": {"L": [{"N": "1"}, {"S": "foo"}]}}
    ) == UniversalRecord(pk={}, typed={"foo": 84.84}, untyped={"bar": [1.0, "foo"]})
