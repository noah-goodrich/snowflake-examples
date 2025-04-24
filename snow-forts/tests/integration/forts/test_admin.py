"""Integration tests for AdminFort."""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from botocore.stub import Stubber
from forts.admin import AdminFort
from aws.secrets import SecretsManager
from resources.warehouse import WarehouseConfig
from resources.database import DatabaseConfig
from resources.role import RoleConfig
from resources.user import UserConfig
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend
import base64


@pytest.fixture
def mock_secrets_manager():
    """Create a mock SecretsManager."""
    manager = Mock(spec=SecretsManager)
    # Setup default behavior for get_secret
    manager.get_secret.return_value = {
        "SecretString": json.dumps({
            "account": "test_account",
            "host": "test.snowflakecomputing.com",
            "username": "SVC_HOID",
            "private_key": "test_key",
            "role": "HOID"
        })
    }
    return manager


@pytest.fixture
def mock_boto3_session():
    """Mock boto3 session with credentials."""
    with patch('boto3.session.Session') as mock_session:
        mock_client = MagicMock()
        # Mock credentials
        mock_session.return_value.get_credentials.return_value = MagicMock(
            access_key='test-key',
            secret_key='test-secret',
            token='test-token'
        )
        mock_session.return_value.client.return_value = mock_client
        yield mock_client


@pytest.fixture(scope="function")
def admin_fort(snow, mock_secrets_manager) -> AdminFort:
    """Create a fresh Admin stack instance for each test."""
    return AdminFort(snow=snow, environment="dev", secrets_manager=mock_secrets_manager)


@pytest.fixture(scope="function", autouse=True)
def cleanup(admin_fort):
    """Cleanup resources before and after each test."""
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


def test_svc_hoid_user_creation(admin_fort, mock_secrets_manager, mock_boto3_session):
    """Test service account creation with key pair authentication."""
    # Setup mock for secret creation
    mock_boto3_session.exceptions.ResourceNotFoundException = Exception
    mock_boto3_session.get_secret_value.side_effect = mock_boto3_session.exceptions.ResourceNotFoundException()

    # Create service account
    user, secret_name = admin_fort.user_manager.create_service_account(
        name='SVC_HOID',
        role='HOID',
        comment='Service account for administrative automation',
        secret_name='snowflake/admin',
        prefix_with_environment=False
    )

    # Verify user was created
    assert user is not None
    assert user.name == "SVC_HOID"

    # Verify secret was created with correct format
    secret_data = json.loads(
        mock_secrets_manager.get_secret.return_value["SecretString"])
    assert secret_data["username"] == "SVC_HOID"
    assert secret_data["role"] == "HOID"


def test_complete_admin_deployment(admin_fort, mock_secrets_manager, mock_boto3_session):
    """Test end-to-end admin deployment."""
    # Setup mock for secret creation
    mock_boto3_session.exceptions.ResourceNotFoundException = Exception
    mock_boto3_session.get_secret_value.side_effect = mock_boto3_session.exceptions.ResourceNotFoundException()

    # Mock warehouse manager
    mock_warehouse_manager = MagicMock()
    mock_warehouse = MagicMock()
    mock_warehouse.warehouse_size = "XS"
    mock_warehouse.auto_suspend = 1
    mock_warehouse.auto_resume = True
    mock_warehouse_manager.get.return_value = mock_warehouse
    admin_fort.warehouse_manager = mock_warehouse_manager

    # Deploy admin infrastructure
    admin_fort.deploy()

    # Verify HOID role was created
    assert admin_fort.role_manager.get("HOID") is not None

    # Verify SVC_HOID user was created
    assert admin_fort.user_manager.get("SVC_HOID") is not None

    # Verify COSMERE_XS warehouse was created
    warehouse = admin_fort.warehouse_manager.get("COSMERE_XS")
    assert warehouse is not None
    assert warehouse.warehouse_size == "XS"
    assert warehouse.auto_suspend == 1
    assert warehouse.auto_resume is True

    # Verify COSMERE database and schemas were created
    database = admin_fort.database_manager.get("COSMERE")
    assert database is not None

    schemas = admin_fort.snow.session.sql(
        "SHOW SCHEMAS IN DATABASE COSMERE").collect()
    schema_names = [row['name'] for row in schemas]
    for schema in ['LOGS', 'AUDIT', 'ADMIN', 'SECURITY']:
        assert schema in schema_names

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

    # Verify secret was created with correct format
    secret_data = json.loads(
        mock_secrets_manager.get_secret.return_value["SecretString"])
    assert secret_data["username"] == "SVC_HOID"
    assert secret_data["role"] == "HOID"
