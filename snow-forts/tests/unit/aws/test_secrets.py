"""Unit tests for AWS Secrets Manager wrapper."""

import pytest
from unittest.mock import Mock, patch
import boto3
from botocore.exceptions import ClientError
from aws.secrets import SecretsManager


@pytest.fixture
def mock_boto3_session():
    """Create a mock boto3 session."""
    with patch('boto3.Session') as mock_session:
        mock_client = Mock()
        mock_session.return_value.client.return_value = mock_client
        yield mock_client


@pytest.fixture
def secrets_manager(mock_boto3_session):
    """Create a SecretsManager instance with mocked boto3."""
    return SecretsManager()


def test_get_secret_success(secrets_manager, mock_boto3_session):
    """Test successful secret retrieval."""
    expected_secret = {
        "SecretString": '{"username": "test", "password": "secret"}'}
    mock_boto3_session.get_secret_value.return_value = expected_secret

    result = secrets_manager.get_secret("test-secret")
    assert result == expected_secret
    mock_boto3_session.get_secret_value.assert_called_once_with(
        SecretId="test-secret")


def test_get_secret_not_found(secrets_manager, mock_boto3_session):
    """Test handling of non-existent secrets."""
    error_response = {
        'Error': {
            'Code': 'ResourceNotFoundException',
            'Message': 'Secret not found'
        }
    }
    mock_boto3_session.get_secret_value.side_effect = ClientError(
        error_response, 'GetSecretValue')

    with pytest.raises(ValueError, match="Secret test-secret not found"):
        secrets_manager.get_secret("test-secret")


def test_get_secret_other_error(secrets_manager, mock_boto3_session):
    """Test handling of other AWS errors."""
    error_response = {
        'Error': {
            'Code': 'InternalServiceError',
            'Message': 'Internal error'
        }
    }
    mock_boto3_session.get_secret_value.side_effect = ClientError(
        error_response, 'GetSecretValue')

    with pytest.raises(ClientError):
        secrets_manager.get_secret("test-secret")


def test_custom_session():
    """Test SecretsManager with custom boto3 session."""
    custom_session = boto3.Session()
    manager = SecretsManager(session=custom_session)
    assert manager.session == custom_session
