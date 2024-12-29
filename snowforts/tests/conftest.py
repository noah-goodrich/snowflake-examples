import pytest
from snowflake.core import Root
from snowflake.snowpark import Session
from typing import Generator
import boto3
import json


@pytest.fixture(scope="session")
def snow() -> Generator[Root, None, None]:
    """Create a reusable Snowflake session for tests.

    Returns:
        Root: Authenticated Snowflake connection

    Notes:
        - Uses session scope to maintain single connection across tests
        - Automatically closes connection after all tests complete
        - Uses test environment credentials
    """

    # Get credentials from AWS Secrets Manager
    secret_name = "snowflake/accountadmin"  # adjust this to your secret name
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')

    """
    aws secretsmanager create-secret --name snowflake/accountadmin --secret-string='{"username":"<username>","password":"<password>","account":"<account>","host":"<host>","role":"ACCOUNTADMIN"}'
    
    """
    try:
        secret_value = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(secret_value['SecretString'])
    except Exception as e:
        raise Exception(
            f"Failed to get Snowflake credentials from Secrets Manager: {e}")

    # Create connection to Snowflake
    session = Session.builder.configs({
        "account": secret['account'],
        "host": secret['host'],
        "user": secret['username'],
        "password": secret['password'],
        "role": secret['role'],
        # default to COMPUTE_WH if not specified
        "warehouse": 'COMPUTE_WH'
    }).create()

    snow = Root(session)

    yield snow

    # Cleanup: close connection after all tests complete
    session.close()
