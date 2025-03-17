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

    # Create a mock for the sql function
    mock_sql = MagicMock()

    def mock_sql_side_effect(query):
        # Create a new DataFrame mock for each query
        mock_df = MagicMock()

        # Configure collect() based on the query
        if "SHOW GRANTS TO ROLE" in query:
            role_name = query.split()[-1]
            if role_name == "HOID":
                mock_df.collect.return_value = [
                    {'role': 'SECURITYADMIN'},
                    {'role': 'SYSADMIN'}
                ]
        elif "SHOW DATABASES" in query:
            mock_df.collect.return_value = [
                {'name': 'DEV_BRONZE'},
                {'name': 'DEV_SILVER'},
                {'name': 'DEV_GOLD'},
                {'name': 'DEV_PLATINUM'}
            ]
        elif "DESC USER" in query:
            mock_df.collect.return_value = [
                {'property': 'DEFAULT_ROLE', 'value': 'HOID'},
                {'property': 'DISABLED', 'value': 'false'},
                {'property': 'RSA_PUBLIC_KEY_FP', 'value': 'test_fingerprint'}
            ]
        elif "DESC WAREHOUSE" in query:
            mock_df.collect.return_value = [
                {'property': 'WAREHOUSE_SIZE', 'value': 'XSMALL'},
                {'property': 'AUTO_SUSPEND', 'value': '1'},
                {'property': 'AUTO_RESUME', 'value': 'true'}
            ]
        elif "SHOW SCHEMAS" in query:
            mock_df.collect.return_value = [
                {'name': 'LOGS'},
                {'name': 'AUDIT'},
                {'name': 'ADMIN'},
                {'name': 'SECURITY'}
            ]
        else:
            mock_df.collect.return_value = []

        return mock_df

    mock_sql.side_effect = mock_sql_side_effect
    session.sql = mock_sql

    # Set up nested _conn attributes needed by Root
    rest = MagicMock()
    rest._host = "test-account.snowflakecomputing.com"  # Set proper hostname format
    rest._port = 443
    rest._scheme = "https"

    conn = MagicMock()
    conn._conn = MagicMock()
    conn._conn.rest = rest

    session._conn = conn

    # Add methods needed by tests
    session.get_current_account = MagicMock(return_value="TEST_ACCOUNT")
    session.get_current_role = MagicMock(return_value="ACCOUNTADMIN")

    # Mock the Session.builder chain
    mock_builder = MagicMock()
    mock_builder.configs.return_value = mock_builder
    mock_builder.create.return_value = session
    Session.builder = mock_builder

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
        created_warehouses = {}

        def mock_warehouse_create(*args, **kwargs):
            snowflake_warehouse = args[0]
            new_warehouse = MagicMock()
            new_warehouse.name = snowflake_warehouse.name
            new_warehouse.warehouse_size = snowflake_warehouse.warehouse_size
            new_warehouse.auto_suspend = snowflake_warehouse.auto_suspend
            new_warehouse.auto_resume = str(
                snowflake_warehouse.auto_resume).lower()
            # Enable query acceleration for larger warehouses
            if snowflake_warehouse.warehouse_size in ['XLARGE', 'XXLARGE', 'XXXLARGE']:
                new_warehouse.enable_query_acceleration = True
                new_warehouse.query_acceleration_max_scale_factor = 8
            # Add drop method
            new_warehouse.drop = MagicMock()
            created_warehouses[snowflake_warehouse.name] = new_warehouse
            return new_warehouse

        def mock_warehouse_get(name):
            if name in created_warehouses:
                return created_warehouses[name]
            # Create a warehouse with the requested name
            new_warehouse = MagicMock()
            new_warehouse.name = name
            # Extract size from name (e.g., DEV_BRONZE_XSMALL -> XSMALL)
            size = name.split('_')[-1]
            new_warehouse.warehouse_size = size
            new_warehouse.auto_suspend = None
            new_warehouse.auto_resume = 'true'
            # Enable query acceleration for larger warehouses
            if size in ['XLARGE', 'XXLARGE', 'XXXLARGE']:
                new_warehouse.enable_query_acceleration = True
                new_warehouse.query_acceleration_max_scale_factor = 8
            # Add drop method
            new_warehouse.drop = MagicMock()
            created_warehouses[name] = new_warehouse
            return new_warehouse

        mock_warehouses.create.side_effect = mock_warehouse_create
        mock_warehouses.__getitem__.side_effect = mock_warehouse_get

        # Setup roles with proper name handling
        mock_roles = MagicMock()
        created_roles = {}

        def mock_role_create(*args, **kwargs):
            snowflake_role = args[0]
            new_role = MagicMock()
            new_role.name = snowflake_role.name
            # Ensure name property returns the actual name string
            type(new_role).name = PropertyMock(
                return_value=snowflake_role.name)
            # Make the role mock return its name when converted to string
            new_role.__str__ = lambda self: snowflake_role.name
            # Store the role for later lookup
            created_roles[snowflake_role.name] = new_role
            return new_role

        def mock_role_getitem(name):
            if name in created_roles:
                return created_roles[name]
            raise KeyError(f"Role {name} not found")

        mock_roles.create.side_effect = mock_role_create
        mock_roles.__getitem__.side_effect = mock_role_getitem

        mock_users = MagicMock()
        mock_user = MagicMock()
        mock_user.name = None
        mock_users.create.return_value = mock_user

        # Configure mock to store and return values
        def mock_user_create(*args, **kwargs):
            snowflake_user = args[0]
            mock_user.name = snowflake_user.name
            mock_user.default_role = snowflake_user.default_role
            return mock_user

        mock_users.create.side_effect = mock_user_create

        # Patch the properties
        type(root).databases = property(lambda self: mock_databases)
        type(root).warehouses = property(lambda self: mock_warehouses)
        type(root).roles = property(lambda self: mock_roles)
        type(root).users = property(lambda self: mock_users)

        # Add hostname attribute
        root._hostname = "test-account.snowflakecomputing.com"

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


@pytest.fixture(scope="function")
def mock_boto3_client(aws_credentials):
    """Mock AWS Secrets Manager client"""
    with mock_aws():
        # Create a real boto3 client that uses moto's mock backend
        client = boto3.client('secretsmanager')

        # Patch boto3.session.Session to return our mocked client
        with patch('boto3.session.Session') as mock_session:
            mock_session.return_value.client.return_value = client
            yield client
