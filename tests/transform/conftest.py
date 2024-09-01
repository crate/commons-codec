import pytest

RESET_TABLES = [
    "from.dynamodb",
]


@pytest.fixture(scope="function")
def cratedb(cratedb_service):
    """
    Provide a fresh canvas to each test case invocation, by resetting database content.
    """
    cratedb_service.reset(tables=RESET_TABLES)
    yield cratedb_service
