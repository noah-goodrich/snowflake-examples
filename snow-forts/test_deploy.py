"""Test script for AdminFort deployment."""

import boto3
import json
from snowflake.core import Root
from snowflake.snowpark import Session
from forts.admin import AdminFort


def get_snowflake_session():
    """Get a Snowflake session using stored credentials."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    secret = client.get_secret_value(SecretId='snowflake/accountadmin')
    secret_value = json.loads(secret['SecretString'])

    # Create session config
    session_config = {
        "account": secret_value['account'],
        "host": secret_value['host'],
        "user": secret_value['username'],
        "private_key": secret_value['private_key'],
        "role": secret_value['role'],
        "warehouse": "COMPUTE_WH"  # Default warehouse
    }

    # Create and return session
    snowpark_session = Session.builder.configs(session_config).create()
    return Root(snowpark_session)


def main():
    """Test the AdminFort deployment."""
    try:
        # Get Snowflake session
        snow = get_snowflake_session()

        # Create AdminFort instance
        admin = AdminFort(snow=snow, environment="dev")

        # Deploy admin infrastructure
        print("Deploying admin infrastructure...")
        admin.deploy()
        print("Admin infrastructure deployed successfully!")

    except Exception as e:
        print(f"Error deploying admin infrastructure: {str(e)}")
        raise


if __name__ == "__main__":
    main()
