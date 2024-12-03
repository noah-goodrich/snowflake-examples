import pytest
import subprocess
from snowflake.core import Root
from snowflake.snowpark import Session
from typing import Generator
import yaml


@pytest.fixture(scope="session")
def env_config() -> dict:
    with open('stacks/config/environments.yaml', 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def admin_config() -> dict:
    with open('stacks/snowflake/config/admin.yaml', 'r') as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="session")
def snow() -> Generator[Root, None, None]:
    """Create a reusable Snowflake session for tests.

    Returns:
        Root: Authenticated Snowflake connection

    Notes:
        - Uses session scope to maintain single connection across tests
        - Automatically closes connection after all tests complete
        - Uses test environment credentials
    """

    # Create connection to LocalStack Snowflake instance
    session = Session.builder.configs({
        "account": "test",
        "host": "snowflake.localhost.localstack.cloud",
        "user": "test",
        "password": "test",
        "role": "test",
        "warehouse": "test",
        "database": "test"
    }).create()

    snow = Root(session)

    yield snow

    # Cleanup: close connection after all tests complete
    session.close()
