#!/usr/bin/env python3
"""Utility script to generate and store Snowflake credentials in AWS Secrets Manager."""

import argparse
import json
import logging
import boto3
from botocore.exceptions import ClientError

from resources.user import User

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_secret(account: str, username: str, region: str) -> None:
    """Create a secret in AWS Secrets Manager with Snowflake credentials.

    Args:
        account: Snowflake account identifier
        username: Snowflake username
        region: AWS region (e.g., us-east-1)
    """
    # Generate key pair
    private_key_pem, public_key_pem = User.generate_key_pair()

    # Construct Snowflake URL
    host = f"{account}.{region}.snowflakecomputing.com"

    # Prepare secret value
    secret_value = {
        "account": account,
        "host": host,
        "username": username,
        "private_key": private_key_pem,
        "role": "ACCOUNTADMIN"  # Default to ACCOUNTADMIN role
    }

    # Create secret in AWS Secrets Manager
    session = boto3.session.Session()
    client = session.client('secretsmanager')

    secret_name = 'snowflake/accountadmin'

    try:
        # Check if secret already exists
        try:
            client.describe_secret(SecretId=secret_name)
            logger.warning(
                f"Secret '{secret_name}' already exists. Updating...")
            client.update_secret(
                SecretId=secret_name,
                SecretString=json.dumps(secret_value)
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"Creating new secret '{secret_name}'...")
                client.create_secret(
                    Name=secret_name,
                    SecretString=json.dumps(secret_value),
                    Description='Snowflake account admin credentials'
                )
            else:
                raise

        logger.info(f"Successfully created/updated secret '{secret_name}'")
        logger.info("Public key (to be added to Snowflake user):")
        logger.info("-" * 80)
        logger.info(public_key_pem)
        logger.info("-" * 80)

    except Exception as e:
        logger.error(f"Failed to create/update secret: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Generate and store Snowflake credentials in AWS Secrets Manager'
    )
    parser.add_argument('--account', required=True,
                        help='Snowflake account identifier')
    parser.add_argument('--username', required=True,
                        help='Snowflake username')
    parser.add_argument('--region', required=True,
                        help='AWS region (e.g., us-east-1)')

    args = parser.parse_args()

    try:
        create_secret(args.account, args.username, args.region)
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        exit(1)


if __name__ == '__main__':
    main()
