import pytest
import boto3
import json
from aws_cdk import App
from stacks.snowflake.snowflake_stack import SnowflakeStack


@pytest.fixture
def secrets_client():
    """Create a boto3 client connected to LocalStack"""
    return boto3.client(
        'secretsmanager',
        endpoint_url='http://localhost:4566',  # LocalStack default endpoint
        region_name='us-east-1',
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )


@pytest.fixture
def accountadmin_secret(secrets_client):
    """Setup ACCOUNTADMIN test secret in LocalStack"""
    secret_value = {
        'account': 'test-account',
        'username': 'test-admin',
        'password': 'test-password'
    }

    secrets_client.create_secret(
        Name='snowflake/accountadmin',
        SecretString=json.dumps(secret_value)
    )

    yield secret_value

    # Cleanup
    secrets_client.delete_secret(
        SecretId='snowflake/accountadmin',
        ForceDeleteWithoutRecovery=True
    )


@pytest.fixture
def svc_admin_secret(secrets_client):
    """Setup service admin test secret in LocalStack"""
    secret_value = {
        'account': 'test-account',
        'username': 'svc-admin',
        'password': 'test-password'
    }

    secrets_client.create_secret(
        Name='snowflake/svc-admin',
        SecretString=json.dumps(secret_value)
    )

    yield secret_value

    # Cleanup
    secrets_client.delete_secret(
        SecretId='snowflake/svc-admin',
        ForceDeleteWithoutRecovery=True
    )


@pytest.fixture
def basic_config():
    """Basic stack configuration"""
    return {
        'environments': {
            'dev': {
                'databases': [{
                    'name': 'ANALYTICS',
                    'schemas': ['RAW']
                }],
                'warehouses': [{
                    'name': 'COMPUTE_WH',
                    'size': 'XSMALL'
                }]
            }
        }
    }


def test_get_accountadmin_secret(accountadmin_secret):
    """Test retrieving ACCOUNTADMIN secret"""
    app = App()
    stack = SnowflakeStack(app, "TestStack")

    result = stack._get_accountadmin_secret()

    assert result == accountadmin_secret
    assert 'account' in result
    assert 'username' in result
    assert 'password' in result


def test_get_svc_admin_secret(svc_admin_secret):
    """Test retrieving service admin secret"""
    app = App()
    stack = SnowflakeStack(app, "TestStack")

    result = stack._get_svc_admin_secret()

    assert result == svc_admin_secret
    assert 'account' in result
    assert 'username' in result
    assert 'password' in result


def test_missing_secret():
    """Test handling of missing secret"""
    app = App()
    stack = SnowflakeStack(app, "TestStack")

    with pytest.raises(Exception) as exc_info:
        stack._get_accountadmin_secret()

    assert "Secret not found" in str(exc_info.value)


def test_invalid_secret_format(secrets_client):
    """Test handling of malformed secret"""
    # Create secret with invalid format
    secrets_client.create_secret(
        Name='snowflake/accountadmin',
        SecretString='invalid-json'
    )

    app = App()
    stack = SnowflakeStack(app, "TestStack")

    with pytest.raises(Exception) as exc_info:
        stack._get_accountadmin_secret()

    # Cleanup
    secrets_client.delete_secret(
        SecretId='snowflake/accountadmin',
        ForceDeleteWithoutRecovery=True
    )


def test_stack_initialization(snow, accountadmin_secret, svc_admin_secret, basic_config):
    """Test complete stack initialization"""
    app = App()
    stack = SnowflakeStack(app, "TestStack")
    stack.admin_config = basic_config

    # Test full initialization
    stack.deploy()

    # Verify components were created
    assert stack.snow is not None
    assert len(stack.databases) > 0
    assert 'DEV_ANALYTICS' in stack.databases
    assert 'DEV_COMPUTE_WH' in stack.warehouses


def test_stack_cleanup(snow, accountadmin_secret, svc_admin_secret):
    """Test stack cleanup on failure"""
    app = App()
    stack = SnowflakeStack(app, "TestStack")

    # Force an error in deployment
    stack.admin_config = {'environments': {}}  # Invalid config

    with pytest.raises(Exception):
        stack.deploy()

    # Verify connection was cleaned up
    assert stack.snow is None
