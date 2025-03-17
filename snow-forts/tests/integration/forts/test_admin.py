import pytest
import boto3
import json
from unittest.mock import patch, MagicMock
from forts.admin import AdminFort
from resources.warehouse import WarehouseConfig
from resources.database import DatabaseConfig
from resources.role import RoleConfig
from resources.user import UserConfig
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import base64


@pytest.fixture(scope="function")
def admin_fort(snow) -> AdminFort:
    """Create a fresh Admin stack instance for each test"""
    return AdminFort(snow=snow, environment="dev")


@pytest.fixture
def mock_boto3_client():
    """Mock boto3 client for Secrets Manager"""
    with patch('boto3.session.Session') as mock_session:
        mock_client = MagicMock()
        mock_session.return_value.client.return_value = mock_client
        yield mock_client


@pytest.fixture(scope="function", autouse=True)
def cleanup(admin_fort):
    """Cleanup resources before and after each test"""
    try:
        admin_fort.warehouse_manager.drop("COSMERE_XS")
        admin_fort.database_manager.drop("COSMERE", cascade=True)
        admin_fort.role_manager.drop("HOID", cascade=True)
        admin_fort.user_manager.drop("SVC_HOID")
    except Exception as e:
        print(f"Setup cleanup error: {e}")

    yield

    try:
        admin_fort.warehouse_manager.drop("COSMERE_XS")
        admin_fort.database_manager.drop("COSMERE", cascade=True)
        admin_fort.role_manager.drop("HOID", cascade=True)
        admin_fort.user_manager.drop("SVC_HOID")
    except Exception as e:
        print(f"Cleanup error: {e}")


def test_cosmere_xs_warehouse_creation(admin_fort):
    """Test end-to-end COSMERE_XS warehouse creation"""
    # Deploy the warehouse
    warehouse = admin_fort.warehouse_manager.create(WarehouseConfig(
        name='COSMERE_XS',
        size='XSMALL',
        auto_suspend=1,
        auto_resume=True,
        prefix_with_environment=False
    ))

    # Verify warehouse exists with correct properties
    assert warehouse is not None
    assert warehouse.name == "COSMERE_XS"
    assert warehouse.warehouse_size == "XSMALL"
    assert warehouse.auto_suspend == 1
    assert warehouse.auto_resume == 'true'


def test_cosmere_database_creation(admin_fort):
    """Test end-to-end COSMERE database creation with schemas"""
    # Deploy the database
    database = admin_fort.database_manager.create(DatabaseConfig(
        name='COSMERE',
        schemas=['LOGS', 'AUDIT', 'ADMIN', 'SECURITY'],
        comment='Administrative database for platform management',
        prefix_with_environment=False
    ))

    # Verify database exists
    assert database is not None
    assert database.name == "COSMERE"

    # Verify schemas
    schemas = [schema.name for schema in database.schemas]
    assert "LOGS" in schemas
    assert "AUDIT" in schemas
    assert "ADMIN" in schemas
    assert "SECURITY" in schemas


def test_hoid_role_creation(admin_fort):
    """Test end-to-end HOID role creation with privileges"""
    # Deploy the role
    role = admin_fort.role_manager.create(RoleConfig(
        name='HOID',
        comment='Administrative role for COSMERE',
        granted_roles=['SECURITYADMIN', 'SYSADMIN'],
        prefix_with_environment=False
    ))

    # Verify role exists
    assert role is not None
    assert role.name == "HOID"

    # Verify privileges
    grants = admin_fort.snow.session.sql("SHOW GRANTS TO ROLE HOID").collect()
    roles = [row['role'] for row in grants]
    assert "SECURITYADMIN" in roles
    assert "SYSADMIN" in roles


def test_svc_hoid_user_creation(admin_fort, mock_boto3_client):
    """Test end-to-end service account creation"""
    # Mock AWS secrets
    mock_boto3_client.get_secret_value.return_value = {
        'SecretString': '{"private_key": "test_key"}'
    }

    # Create service account
    user, secret_name = admin_fort.user_manager.create_service_account(
        name='SVC_HOID',
        role='HOID',
        comment='Service account for administrative automation',
        secret_name='snowflake/admin',
        prefix_with_environment=False
    )

    # Verify user exists
    assert user is not None
    assert user.name == "SVC_HOID"
    assert user.default_role == "HOID"


def test_complete_admin_deployment(admin_fort, mock_boto3_client):
    """Test end-to-end admin deployment"""
    # Generate a test RSA key pair for mocking
    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend()
    )

    # Get private key in PEM format
    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    ).decode('utf-8')

    # Get public key in PEM format
    public_key_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    # Mock AWS Secrets Manager for storing credentials
    stored_secret = json.dumps({
        "username": "SVC_HOID",
        "private_key": private_key_pem,  # Store as PEM string
        "role": "HOID",
        "account": "test-account",
        "host": "test-account.snowflakecomputing.com"
    })

    # Mock the secret not existing initially, then existing after creation
    mock_boto3_client.exceptions.ResourceNotFoundException = Exception
    secret_exists = False

    def mock_get_secret(**kwargs):
        nonlocal secret_exists
        if not secret_exists:
            raise mock_boto3_client.exceptions.ResourceNotFoundException()
        return {'SecretString': stored_secret}

    def mock_create_secret(**kwargs):
        nonlocal secret_exists, stored_secret
        secret_exists = True
        stored_secret = kwargs['SecretString']
        return {'ARN': 'test-arn', 'Name': kwargs['Name']}

    mock_boto3_client.get_secret_value.side_effect = mock_get_secret
    mock_boto3_client.create_secret.side_effect = mock_create_secret

    # Deploy everything
    admin_fort.deploy()

    # Verify core components exist
    assert admin_fort.role_manager.get("HOID") is not None
    assert admin_fort.user_manager.get("SVC_HOID") is not None
    assert admin_fort.warehouse_manager.get("COSMERE_XS") is not None
    assert admin_fort.database_manager.get("COSMERE") is not None

    # Verify HOID role has required system privileges
    grants = admin_fort.snow.session.sql("SHOW GRANTS TO ROLE HOID").collect()
    granted_roles = [row['role'] for row in grants]
    assert "SECURITYADMIN" in granted_roles
    assert "SYSADMIN" in granted_roles

    # Verify SVC_HOID user configuration
    user_desc = admin_fort.snow.session.sql("DESC USER SVC_HOID").collect()
    user_props = {row['property']: row['value'] for row in user_desc}
    assert user_props['DEFAULT_ROLE'] == 'HOID'
    assert user_props['DISABLED'] == 'false'
    assert 'RSA_PUBLIC_KEY_FP' in user_props  # Verify key was set

    # Verify COSMERE_XS warehouse configuration
    wh_desc = admin_fort.snow.session.sql(
        "DESC WAREHOUSE COSMERE_XS").collect()
    wh_props = {row['property']: row['value'] for row in wh_desc}
    assert wh_props['WAREHOUSE_SIZE'] == 'XSMALL'
    assert wh_props['AUTO_SUSPEND'] == '1'
    assert wh_props['AUTO_RESUME'] == 'true'

    # Verify COSMERE database and schemas
    schemas = admin_fort.snow.session.sql(
        "SHOW SCHEMAS IN DATABASE COSMERE").collect()
    schema_names = [row['name'] for row in schemas]
    for schema in ['LOGS', 'AUDIT', 'ADMIN', 'SECURITY']:
        assert schema in schema_names

    # Verify secret was created with correct format
    mock_boto3_client.create_secret.assert_called_once()
    secret_call = mock_boto3_client.create_secret.call_args[1]
    assert secret_call['Name'] == "snowflake/admin"

    # Parse and verify secret contents
    secret_data = json.loads(secret_call['SecretString'])
    assert secret_data['username'] == "SVC_HOID"
    assert "-----BEGIN PRIVATE KEY-----" in secret_data['private_key']
    assert "-----END PRIVATE KEY-----" in secret_data['private_key']
    assert secret_data['role'] == "HOID"
    assert secret_data['account'] == admin_fort.snow.session.get_current_account(
    ).replace('"', '')
    assert secret_data['host'] == admin_fort.snow._hostname
