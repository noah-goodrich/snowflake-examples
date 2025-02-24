import pytest
from unittest.mock import patch, MagicMock
from forts.admin import AdminFort
from resources.warehouse import WarehouseConfig
from resources.database import DatabaseConfig
from resources.role import RoleConfig
from resources.user import UserConfig


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


def test_admin_role_creation(admin_fort):
    """Test HOID role creation with system privileges"""
    # Create admin role
    role = admin_fort.role_manager.create(RoleConfig(
        name='HOID',
        comment='Administrative role for COSMERE',
        granted_roles=['SECURITYADMIN', 'SYSADMIN'],
        prefix_with_environment=False
    ))

    assert role is not None
    assert role.name == "HOID"


def test_service_account_creation(admin_fort, mock_boto3_client):
    """Test service account creation with key pair auth"""
    # Mock secret retrieval
    mock_boto3_client.get_secret_value.return_value = {
        'SecretString': '{"private_key": "test_key"}'
    }

    # Create service account
    user = admin_fort.user_manager.create(UserConfig(
        name='SVC_HOID',
        default_role='HOID',
        rsa_public_key="TEST_KEY",
        comment='Service account for administrative automation',
        prefix_with_environment=False
    ))

    assert user is not None
    assert user.name == "SVC_HOID"
    assert user.default_role == "HOID"


def test_admin_warehouse_creation(admin_fort):
    """Test admin warehouse creation"""
    # Create admin warehouse
    warehouse = admin_fort.warehouse_manager.create(WarehouseConfig(
        name='COSMERE_XS',
        size='XSMALL',
        auto_suspend=1,
        auto_resume=True,
        prefix_with_environment=False
    ))

    assert warehouse is not None
    assert warehouse.name == "COSMERE_XS"
    assert warehouse.warehouse_size == "XSMALL"
    assert warehouse.auto_suspend == 1


def test_admin_database_creation(admin_fort):
    """Test admin database creation with schemas"""
    # Create admin database
    database = admin_fort.database_manager.create(DatabaseConfig(
        name='COSMERE',
        schemas=['LOGS', 'AUDIT', 'ADMIN', 'SECURITY'],
        comment='Administrative database for platform management',
        prefix_with_environment=False
    ))

    assert database is not None
    assert database.name == "COSMERE"

    # Verify schemas
    schemas = [schema.name for schema in database.schemas]
    assert "LOGS" in schemas
    assert "AUDIT" in schemas
    assert "ADMIN" in schemas
    assert "SECURITY" in schemas
