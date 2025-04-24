"""Tests for the key management service."""

from unittest.mock import MagicMock, patch
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa as crypto_rsa
from cryptography.hazmat.backends import default_backend

from services.key_management import KeyManagementService, AWSSecretsManagerAdapter


@pytest.fixture
def mock_aws_client():
    """Create a mock AWS Secrets Manager client."""
    client = MagicMock()
    client.create_secret = MagicMock()
    client.update_secret = MagicMock()
    client.get_secret_value = MagicMock()
    client.delete_secret = MagicMock()
    client.exceptions = MagicMock()
    client.exceptions.ResourceExistsException = Exception
    client.exceptions.ResourceNotFoundException = Exception
    return client


@pytest.fixture
def mock_adapter():
    """Create a mock adapter for key management service tests."""
    adapter = MagicMock()
    adapter.store_secret = MagicMock()
    adapter.retrieve_secret = MagicMock()
    adapter.delete_secret = MagicMock()
    return adapter


@pytest.fixture
def aws_adapter(mock_aws_client):
    """Create an AWS Secrets Manager adapter."""
    return AWSSecretsManagerAdapter(mock_aws_client)


@pytest.fixture
def key_management_service(mock_adapter):
    """Create a KeyManagementService instance."""
    return KeyManagementService(mock_adapter)


def test_generate_key_pair(key_management_service, mock_adapter):
    """Test generating a new key pair."""
    # Generate key pair
    public_key, private_key = key_management_service.generate_key_pair(
        "test_user")

    # Verify keys are in PEM format
    assert "-----BEGIN PUBLIC KEY-----" in public_key
    assert "-----BEGIN PRIVATE KEY-----" in private_key

    # Verify private key was stored
    mock_adapter.store_secret.assert_called_once_with(
        "test_user_private_key",
        private_key
    )


def test_rotate_keys(key_management_service, mock_adapter):
    """Test rotating an existing key pair."""
    # Generate initial key pair
    old_public_key, _ = key_management_service.generate_key_pair("test_user")
    mock_adapter.store_secret.reset_mock()

    # Rotate keys
    new_public_key, new_private_key = key_management_service.rotate_keys(
        "test_user")

    # Verify new keys are different
    assert new_public_key != old_public_key

    # Verify old key was deleted and new key was stored
    mock_adapter.delete_secret.assert_called_once_with("test_user_private_key")
    mock_adapter.store_secret.assert_called_once_with(
        "test_user_private_key",
        new_private_key
    )


def test_validate_key_pair(key_management_service, mock_adapter):
    """Test validating a key pair."""
    # Generate key pair
    public_key, private_key = key_management_service.generate_key_pair(
        "test_user")

    # Mock storage to return our private key
    mock_adapter.retrieve_secret.return_value = private_key

    # Validate key pair
    assert key_management_service.validate_key_pair("test_user", public_key)

    # Verify with invalid public key
    invalid_public_key = public_key.replace("A", "B")
    assert not key_management_service.validate_key_pair(
        "test_user", invalid_public_key)


def test_aws_adapter_store_secret(mock_aws_client, aws_adapter):
    """Test storing a secret in AWS."""
    # Store new secret
    aws_adapter.store_secret("test_secret", "test_value")
    mock_aws_client.create_secret.assert_called_once_with(
        Name="test_secret",
        SecretString="test_value"
    )

    # Reset mock and simulate existing secret
    mock_aws_client.create_secret.reset_mock()
    mock_aws_client.create_secret.side_effect = mock_aws_client.exceptions.ResourceExistsException()
    aws_adapter.store_secret("test_secret", "new_value")
    mock_aws_client.update_secret.assert_called_once_with(
        SecretId="test_secret",
        SecretString="new_value"
    )


def test_aws_adapter_retrieve_secret(mock_aws_client, aws_adapter):
    """Test retrieving a secret from AWS."""
    # Mock successful retrieval
    mock_aws_client.get_secret_value.return_value = {
        "SecretString": "test_value"}
    assert aws_adapter.retrieve_secret("test_secret") == "test_value"

    # Mock secret not found
    mock_aws_client.get_secret_value.side_effect = mock_aws_client.exceptions.ResourceNotFoundException()
    with pytest.raises(Exception) as exc_info:
        aws_adapter.retrieve_secret("test_secret")
    assert "Secret test_secret not found" in str(exc_info.value)


def test_aws_adapter_delete_secret(mock_aws_client, aws_adapter):
    """Test deleting a secret from AWS."""
    # Delete existing secret
    aws_adapter.delete_secret("test_secret")
    mock_aws_client.delete_secret.assert_called_once_with(
        SecretId="test_secret",
        ForceDeleteWithoutRecovery=True
    )

    # Reset mock and simulate nonexistent secret
    mock_aws_client.delete_secret.reset_mock()
    mock_aws_client.delete_secret.side_effect = mock_aws_client.exceptions.ResourceNotFoundException()
    aws_adapter.delete_secret("test_secret")  # Should not raise exception
