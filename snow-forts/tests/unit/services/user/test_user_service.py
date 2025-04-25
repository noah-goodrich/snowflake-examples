"""Tests for the user service."""

from unittest.mock import MagicMock, patch
import pytest
from services.user import UserService
from services.key_management import KeyManagementService, AWSSecretsManagerAdapter
from specs.user import UserSpec, UserType
from snowflake.core.user import User


@pytest.fixture
def mock_aws_client():
    """Mock AWS Secrets Manager client."""
    return MagicMock()


@pytest.fixture
def aws_adapter(mock_aws_client):
    """Create AWS adapter with mocked client."""
    from services.key_management.adapters.aws import AWSSecretsManagerAdapter
    return AWSSecretsManagerAdapter(mock_aws_client)


@pytest.fixture
def key_management_service(aws_adapter):
    """Create key management service with mocked adapter."""
    return KeyManagementService(aws_adapter)


@pytest.fixture
def mock_root():
    """Mock Snowflake root object."""
    root = MagicMock()
    root.users = MagicMock()
    root.roles = MagicMock()
    return root


@pytest.fixture
def user_service(mock_root, key_management_service):
    """Create user service with mocked dependencies."""
    return UserService(mock_root, key_management_service)


def test_create_regular_user(user_service, mock_root, key_management_service):
    """Test creating a regular user."""
    # Mock user creation
    mock_user = MagicMock()
    mock_user.name = "test_user"
    mock_user.user_type = "regular"
    mock_user.comment = "Test user"
    mock_user.default_warehouse = "test_wh"
    mock_user.default_namespace = "test_ns"
    mock_user.default_role = "test_role"
    mock_user.disabled = False
    mock_user.must_change_password = True
    mock_root.users.create.return_value = mock_user

    # Create user spec
    spec = UserSpec(
        name="test_user",
        role="test_role",
        user_type=UserType.REGULAR,
        comment="Test user",
        default_warehouse="test_wh",
        default_namespace="test_ns",
        default_role="test_role",
        disabled=False,
        must_change_password=True
    )

    # Create user
    result = user_service.create(spec)

    # Verify user was created with correct properties
    mock_root.users.create.assert_called_once()
    created_user = mock_root.users.create.call_args[0][0]
    assert isinstance(created_user, User)
    assert created_user.name == "test_user"
    assert created_user.comment == "Test user"
    assert created_user.default_warehouse == "test_wh"
    assert created_user.default_namespace == "test_ns"
    assert created_user.default_role == "test_role"
    assert created_user.disabled is False
    assert created_user.must_change_password is True

    # Verify result
    assert result["name"] == "test_user"
    assert result["user_type"] == "regular"
    assert result["comment"] == "Test user"
    assert result["default_warehouse"] == "test_wh"
    assert result["default_namespace"] == "test_ns"
    assert result["default_role"] == "test_role"
    assert result["disabled"] is False
    assert result["must_change_password"] is True


def test_create_service_account(user_service, mock_root, key_management_service):
    """Test creating a service account."""
    # Mock user creation
    mock_user = MagicMock()
    mock_user.name = "test_service"
    mock_user.comment = "Test service account"
    mock_user.default_warehouse = "test_wh"
    mock_user.default_namespace = "test_ns"
    mock_user.default_role = "test_role"
    mock_user.disabled = False
    mock_root.users.create.return_value = mock_user

    # Mock key management service methods
    key_management_service.generate_key_pair.return_value = (
        "test_public_key", "test_private_key")
    key_management_service.validate_key_pair.return_value = True

    # Create user spec
    spec = UserSpec(
        name="test_service",
        role="test_role",
        user_type=UserType.SERVICE_ACCOUNT,
        comment="Test service account",
        default_warehouse="test_wh",
        default_namespace="test_ns",
        default_role="test_role",
        disabled=False
    )

    # Create user
    result = user_service.create(spec)

    # Verify user was created with correct properties
    mock_root.users.create.assert_called_once()
    created_user = mock_root.users.create.call_args[0][0]
    assert isinstance(created_user, User)
    assert created_user.name == "test_service"
    assert created_user.comment == "Test service account"
    assert created_user.default_warehouse == "test_wh"
    assert created_user.default_namespace == "test_ns"
    assert created_user.default_role == "test_role"
    assert created_user.disabled is False

    # Verify key pair was generated
    key_management_service.generate_key_pair.assert_called_once_with(
        "test_service")

    # Verify result
    assert result["name"] == "test_service"
    assert result["user_type"] == "service_account"
    assert result["comment"] == "Test service account"
    assert result["default_warehouse"] == "test_wh"
    assert result["default_namespace"] == "test_ns"
    assert result["default_role"] == "test_role"
    assert result["disabled"] is False


def test_alter_user(user_service, mock_root, key_management_service):
    """Test altering a regular user."""
    # Mock user update
    mock_user = MagicMock()
    mock_user.name = "test_user"
    mock_user.user_type = "REGULAR"
    mock_root.users.create.return_value = mock_user

    # Create user spec
    spec = UserSpec(
        name="test_user",
        role="new_role",
        user_type=UserType.REGULAR,
        comment="Updated test user",
        default_warehouse="new_wh",
        default_namespace="new_ns",
        default_role="new_role",
        disabled=True,
        must_change_password=False
    )

    # Alter user
    result = user_service.alter(spec)

    # Verify user was updated with correct properties
    mock_root.users.create.assert_called_once()
    updated_user = mock_root.users.create.call_args[0][0]
    assert isinstance(updated_user, User)
    assert updated_user.name == "test_user"
    assert updated_user.comment == "Updated test user"
    assert updated_user.default_warehouse == "new_wh"
    assert updated_user.default_namespace == "new_ns"
    assert updated_user.default_role == "new_role"
    assert updated_user.disabled is True
    assert updated_user.must_change_password is False

    # Verify result
    assert result["name"] == "test_user"
    assert result["user_type"] == "REGULAR"
    assert result["comment"] == "Updated test user"
    assert result["default_warehouse"] == "new_wh"
    assert result["default_namespace"] == "new_ns"
    assert result["default_role"] == "new_role"
    assert result["disabled"] is True
    assert result["must_change_password"] is False


@patch('services.key_management.KeyManagementService')
def test_alter_service_account(mock_key_management_service, user_service, mock_root):
    """Test altering a service account."""
    # Mock user update
    mock_user = MagicMock()
    mock_user.name = "test_service"
    mock_user.user_type = "SERVICE_ACCOUNT"
    mock_root.users.create.return_value = mock_user

    # Mock key rotation
    new_public_key = "-----BEGIN PUBLIC KEY-----\nnew_key\n-----END PUBLIC KEY-----"
    new_private_key = "-----BEGIN PRIVATE KEY-----\nnew_key\n-----END PRIVATE KEY-----"
    mock_key_management_service.return_value.generate_key_pair.return_value = (
        new_public_key, new_private_key)

    # Create user spec
    spec = UserSpec(
        name="test_service",
        role="new_role",
        user_type=UserType.SERVICE_ACCOUNT,
        comment="Updated test service account",
        default_warehouse="new_wh",
        default_namespace="new_ns",
        default_role="new_role",
        disabled=True,
        must_change_password=False
    )

    # Alter user
    result = user_service.alter(spec)

    # Verify user was updated with correct properties
    mock_root.users.create.assert_called_once()
    updated_user = mock_root.users.create.call_args[0][0]
    assert isinstance(updated_user, User)
    assert updated_user.name == "test_service"
    assert updated_user.comment == "Updated test service account"
    assert updated_user.default_warehouse == "new_wh"
    assert updated_user.default_namespace == "new_ns"
    assert updated_user.default_role == "new_role"
    assert updated_user.disabled is True
    assert updated_user.must_change_password is False

    # Verify result
    assert result["name"] == "test_service"
    assert result["user_type"] == "SERVICE_ACCOUNT"
    assert result["comment"] == "Updated test service account"
    assert result["default_warehouse"] == "new_wh"
    assert result["default_namespace"] == "new_ns"
    assert result["default_role"] == "new_role"
    assert result["disabled"] is True
    assert result["must_change_password"] is False
    assert result["public_key"] == new_public_key


def test_drop_user(user_service, mock_root, key_management_service):
    """Test dropping a regular user."""
    # Mock user drop
    mock_root.users.drop.return_value = None

    # Drop user
    result = user_service.drop("test_user")

    # Verify user was dropped
    mock_root.users.drop.assert_called_once_with("test_user")
    assert result is None


@patch('services.key_management.KeyManagementService')
def test_drop_service_account(mock_key_management_service, user_service, mock_root):
    """Test dropping a service account."""
    # Mock user drop
    mock_root.users.drop.return_value = None

    # Mock private key deletion
    mock_key_management_service.return_value.delete_private_key.return_value = None

    # Drop user
    result = user_service.drop("test_service")

    # Verify private key was deleted
    mock_key_management_service.return_value.delete_private_key.assert_called_once_with(
        "test_service")

    # Verify user was dropped
    mock_root.users.drop.assert_called_once_with("test_service")
    assert result is None
