import pytest

from commons_codec.transform.dynamodb import DynamoDBCDCTranslator, DynamoDBFullLoadTranslator
from commons_codec.transform.dynamodb_model import PrimaryKeySchema

RESET_TABLES = [
    "from.dynamodb",
    "from.mongodb",
]


@pytest.fixture(scope="function")
def cratedb(cratedb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    cratedb_service.reset(tables=RESET_TABLES)
    yield cratedb_service


@pytest.fixture
def dynamodb_full_translator_foo():
    return DynamoDBFullLoadTranslator(table_name="foo", primary_key_schema=PrimaryKeySchema().add("id", "S"))


@pytest.fixture
def dynamodb_cdc_translator_foo():
    return DynamoDBCDCTranslator(table_name="foo")
