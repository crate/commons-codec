import os

import pytest

from commons_codec.transform.dynamodb import DynamoDBCDCTranslator, DynamoDBFullLoadTranslator
from commons_codec.transform.dynamodb_model import PrimaryKeySchema
from tests.cratedb import CrateDBTestAdapter

RESET_TABLES = [
    "from.dynamodb",
    "from.generic",
    "from.mongodb",
]


@pytest.fixture(scope="session")
def cratedb_custom_service():
    """
    Provide a CrateDB service instance to the test suite.
    """
    db = CrateDBTestAdapter(crate_version=os.environ.get("CRATEDB_VERSION", "nightly"))
    db.start()
    yield db
    db.stop()


@pytest.fixture(scope="function")
def cratedb(cratedb_custom_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    cratedb_custom_service.reset(tables=RESET_TABLES)
    yield cratedb_custom_service


@pytest.fixture
def dynamodb_full_translator_foo():
    return DynamoDBFullLoadTranslator(table_name="foo", primary_key_schema=PrimaryKeySchema().add("id", "S"))


@pytest.fixture
def dynamodb_cdc_translator_foo():
    return DynamoDBCDCTranslator(table_name="foo")
