"""Integration tests for User resource management."""

import json
import os
import pytest
from unittest.mock import MagicMock, patch
from snowflake.core import Root

from resources.user import User, UserConfig
from aws.secrets import SecretsManager


@pytest.fixture
def snow():
    """Snowflake connection fixture."""
    mock_root = MagicMock(spec=Root)
    mock_root.session = MagicMock()
    mock_root.session.get_current_account.return_value = "test_account"
    mock_root._hostname = "test.snowflakecomputing.com"
    mock_root.users = MagicMock()

    return mock_root


@pytest.fixture
def secrets_manager():
    """SecretsManager fixture."""
    return MagicMock(spec=SecretsManager)


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


@pytest.fixture
def user_manager(snow, secrets_manager, mock_boto3_session):
    """User manager fixture."""
    return User(snow, "test", secrets_manager)


def test_create_service_account(user_manager, mock_boto3_session):
    """Test service account creation with key pair authentication."""
    # Setup mock for secret creation
    mock_boto3_session.exceptions.ResourceNotFoundException = Exception
    mock_boto3_session.get_secret_value.side_effect = mock_boto3_session.exceptions.ResourceNotFoundException()

    # Mock user creation
    mock_user = MagicMock()
    mock_user.name = "TEST_TEST_SERVICE"
    mock_user.comment = "Test service account"
    mock_user.default_role = "test_role"
    user_manager.snow.users.create.return_value = mock_user

    # Create service account
    user, secret_name = user_manager.create_service_account(
        name="test_service",
        role="test_role",
        comment="Test service account",
        secret_name="test/service"
    )

    # Verify user was created
    assert user.name == "TEST_TEST_SERVICE"
    assert user.comment == "Test service account"
    assert user.default_role == "test_role"

    # Verify secret was created
    mock_boto3_session.create_secret.assert_called_once()
    secret_args = mock_boto3_session.create_secret.call_args.kwargs
    assert secret_args["Name"] == "test/service"
    secret_value = json.loads(secret_args["SecretString"])
    assert secret_value["username"] == "TEST_TEST_SERVICE"
    assert secret_value["role"] == "test_role"
    assert "private_key" in secret_value


def test_rotate_service_account_key(user_manager, mock_boto3_session):
    """Test service account key rotation."""
    # Setup mock for secret operations
    mock_boto3_session.get_secret_value.return_value = {
        'SecretString': json.dumps({
            'username': 'TEST_TEST_ROTATE',
            'private_key': 'old_key',
            'role': 'test_role'
        })
    }

    # Mock user
    mock_user = MagicMock()
    mock_user.name = "TEST_TEST_ROTATE"
    user_manager.snow.users.__getitem__.return_value = mock_user
    user_manager.snow.users.create.return_value = mock_user

    # Create service account
    user, secret_name = user_manager.create_service_account(
        name="test_rotate",
        role="test_role",
        secret_name="test/rotate"
    )

    # Create new key pair and rotate
    new_key = "test_key"
    user_manager.rotate_rsa_key("test_rotate", new_key)

    # Verify secret was updated
    mock_boto3_session.put_secret_value.assert_called_once()
    secret_args = mock_boto3_session.put_secret_value.call_args.kwargs
    assert secret_args["SecretId"] == "test/rotate"
    secret_value = json.loads(secret_args["SecretString"])
    assert "private_key" in secret_value
