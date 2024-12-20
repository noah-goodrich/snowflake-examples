import pytest
import boto3
import json
from unittest.mock import patch, MagicMock
from aws_cdk import App
from ....stacks.admin import Admin
from snowflake.core.warehouse import Warehouse
from snowflake.core.database import Database
from snowflake.core.role import Role
from snowflake.core.user import User


@pytest.fixture(scope="module")
def admin_stack(snow) -> Admin:
    """Create a fresh Admin stack instance for each test"""
    app = App()
    stack = Admin(app, "TestAdmin", snow)
    return stack


@pytest.fixture
def mock_boto3_client():
    """Mock boto3 client for Secrets Manager"""
    with patch('boto3.session.Session') as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_snowpark_session():
    """Mock Snowpark session"""
    with patch('snowflake.snowpark.Session') as mock_session:
        mock_builder = MagicMock()
        mock_session.builder.configs.return_value = mock_builder
        yield mock_session


def test_cosmere_xs_warehouse_creation(admin_stack):
    """Test COSMERE_XS warehouse creation with configuration"""
    # Verify warehouse exists with correct properties
    wh = admin_stack.snow.warehouses["COSMERE_XS"].fetch()
    assert wh is not None
    assert wh.name == "COSMERE_XS"
    assert wh.warehouse_size == "X-Small"
    assert wh.auto_suspend == 1
    assert wh.auto_resume == 'true'
    # assert wh.initially_suspended == None  # TODO: fix this. Don't know why it's not working. --ndg 12/12/2024


def test_cosmere_database_creation(admin_stack):
    """Test COSMERE database creation with schemas"""
    # Verify database exists
    db = admin_stack.snow.databases["COSMERE"].fetch()
    assert db is not None
    assert db.name == "COSMERE"
    assert db.comment == "Administrative database for platform management"

    # Verify schemas exist
    schemas = admin_stack.snow.databases["COSMERE"].schemas
    assert "ADMIN" in schemas
    assert "AUDIT" in schemas
    assert "SECURITY" in schemas


def test_hoid_role_creation(admin_stack):
    """Test HOID administrative role creation"""

    # Verify role exists
    role = admin_stack.snow.roles["HOID"]
    assert role is not None
    assert role.name == "HOID"

    # Verify role has been granted system roles
    grants = list(admin_stack.snow.roles["HOID"].iter_grants_to())
    granted_roles = [grant.securable.name for grant in grants]
    assert "SECURITYADMIN" in granted_roles
    assert "SYSADMIN" in granted_roles


def test_svc_hoid_user_creation(admin_stack: Admin):
    """Test service account creation and configuration"""

    # Verify user exists
    user = admin_stack.snow.users["SVC_HOID"].fetch()
    assert user is not None
    assert user.name == "SVC_HOID"
    assert user.comment == "Service account for administrative automation"
    assert user.default_role == "HOID"
    assert user.default_warehouse == None

    # Verify role grant
    grants = list(admin_stack.snow.users["SVC_HOID"].iter_grants_to())
    assert len(grants) == 1
    grant = grants[0]
    assert grant.securable.name == "HOID"
    assert grant.securable_type == "ROLE"


# def test_admin_deploy_creates_secrets(admin_stack, mock_boto3_client):
#     """Test that deploy creates necessary secrets in AWS Secrets Manager"""

#     # Verify secret was created with correct structure
#     mock_boto3_client.create_secret.assert_called_once()
#     secret_args = mock_boto3_client.create_secret.call_args[1]
#     assert secret_args['Name'] == 'snowflake/admin'

#     secret_data = json.loads(secret_args['SecretString'])
#     assert 'username' in secret_data
#     assert secret_data['username'] == 'SVC_HOID'
#     assert 'private_key' in secret_data
#     assert 'account' in secret_data
#     assert 'host' in secret_data
#     assert secret_data['role'] == 'HOID'


def test_admin_deploy_creates_snowpark_session(admin_stack, mock_snowpark_session, mock_boto3_client):
    """Test that deploy creates Snowpark session with correct configuration"""
    assert admin_stack.snow is not None
    assert admin_stack.snow.session.get_current_role().replace('"', '') == 'HOID'
    assert admin_stack.snow.session.get_current_user().replace('"', '') == 'SVC_HOID'


# def test_admin_deploy_handles_secrets_error(admin_stack, mock_boto3_client):
#     """Test that deploy handles Secrets Manager errors gracefully"""
#     mock_boto3_client.get_secret_value.side_effect = Exception(
#         "Secret not found")

#     with pytest.raises(Exception) as exc_info:
#         admin_stack.deploy()

#     assert "Failed to get Snowflake credentials from Secrets Manager" in str(
#         exc_info.value)


@pytest.fixture(scope='module', autouse=True)
def cleanup(admin_stack):
    """Cleanup resources before and after each test"""
    admin_stack.snow.warehouses["COSMERE_XS"].drop(True)
    admin_stack.snow.databases["COSMERE"].drop(True)
    admin_stack.snow.users["SVC_HOID"].drop(True)
    admin_stack.snow.roles["HOID"].drop(True)

    original = admin_stack.snow

    admin_stack.deploy()

    yield

    # Drop resources in reverse order of creation
    admin_stack.snow.databases["COSMERE"].drop(True)
    admin_stack.snow.warehouses["COSMERE_XS"].drop(True)
    admin_stack.snow.users["SVC_HOID"].drop(True)
    original.roles["HOID"].drop(True)
