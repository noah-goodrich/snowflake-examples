import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from snowflake.core import Root
from snowflake.snowpark import Session
import boto3
from moto import mock_aws


@pytest.fixture(scope="function")
def mock_snowpark_session():
    """Mock Snowpark session for unit tests"""
    session = MagicMock(spec=Session)
    session.sql.return_value.collect.return_value = []

    # Set up nested _conn attributes needed by Root
    rest = MagicMock()
    rest._host = "account.snowflakecomputing.com"
    rest._port = 443
    rest._scheme = "https"

    conn = MagicMock()
    conn._conn = MagicMock()
    conn._conn.rest = rest
    session._conn = conn

    # Add methods needed by tests
    session.get_current_account = MagicMock(return_value="TEST_ACCOUNT")
    session.get_current_role = MagicMock(return_value="ACCOUNTADMIN")

    return session


@pytest.fixture(scope="function")
def snow(mock_snowpark_session):
    """Create a mock Root connection with minimal setup"""
    # Patch the session property before creating Root instance
    with patch('snowflake.core.Root.session', new_callable=PropertyMock) as mock_session_prop:
        mock_session_prop.return_value = mock_snowpark_session
        root = Root(mock_snowpark_session)

        # Create mocks for databases, warehouses, and roles
        mock_databases = MagicMock()

        def mock_database_create(*args, **kwargs):
            snowflake_database = args[0]
            new_database = MagicMock()
            new_database.name = snowflake_database.name
            new_database.comment = snowflake_database.comment

            # Create a proper mock for schemas
            schemas_mock = MagicMock()
            created_schemas = []

            def mock_schema_create(*schema_args, **schema_kwargs):
                schema = MagicMock()
                schema.name = schema_args[0].name
                created_schemas.append(schema)
                return schema

            # Set up schemas to work both as a property and for method calls
            schemas_mock.create = mock_schema_create
            schemas_mock.__iter__ = lambda self: iter(created_schemas)
            schemas_mock.__getitem__ = lambda self, idx: created_schemas[idx]

            # Attach schemas to database
            new_database.schemas = schemas_mock

            return new_database

        mock_databases.create.side_effect = mock_database_create

        # Create mocks for warehouses, roles, and users
        mock_warehouses = MagicMock()
        mock_warehouse = MagicMock()
        mock_warehouse.name = None
        mock_warehouse.warehouse_size = None
        mock_warehouse.auto_suspend = None
        mock_warehouses.create.return_value = mock_warehouse

        mock_roles = MagicMock()
        mock_role = MagicMock()
        mock_role.name = None
        mock_roles.create.return_value = mock_role

        mock_users = MagicMock()
        mock_user = MagicMock()
        mock_user.name = None
        mock_users.create.return_value = mock_user

        # Configure mock to store and return values
        def mock_warehouse_create(*args, **kwargs):
            snowflake_warehouse = args[0]
            mock_warehouse.name = snowflake_warehouse.name
            mock_warehouse.warehouse_size = snowflake_warehouse.warehouse_size
            mock_warehouse.auto_suspend = snowflake_warehouse.auto_suspend
            return mock_warehouse

        def mock_role_create(*args, **kwargs):
            snowflake_role = args[0]
            new_role = MagicMock()
            new_role.name = snowflake_role.name
            return new_role

        def mock_user_create(*args, **kwargs):
            snowflake_user = args[0]
            mock_user.name = snowflake_user.name
            mock_user.default_role = snowflake_user.default_role
            return mock_user

        mock_warehouses.create.side_effect = mock_warehouse_create
        mock_roles.create.side_effect = mock_role_create
        mock_users.create.side_effect = mock_user_create

        # Patch the properties
        type(root).databases = property(lambda self: mock_databases)
        type(root).warehouses = property(lambda self: mock_warehouses)
        type(root).roles = property(lambda self: mock_roles)
        type(root).users = property(lambda self: mock_users)

        # Add hostname attribute
        root._hostname = "account.snowflakecomputing.com"

        return root


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    import os
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mocked S3 client"""
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="function")
def sts(aws_credentials):
    """Mocked STS client"""
    with mock_aws():
        yield boto3.client("sts")


@pytest.fixture(scope="function")
def iam(aws_credentials):
    """Mocked IAM client"""
    with mock_aws():
        yield boto3.client("iam")
