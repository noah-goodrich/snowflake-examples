import argparse
import boto3
import json
import logging
from snowflake.snowpark import Session
from snowflake.core import Root
from botocore.exceptions import ClientError

from forts.admin import AdminFort
from forts.medallion import MedallionFort

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_secret_exists(secret_name: str) -> bool:
    """Verify that the specified secret exists in AWS Secrets Manager"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')

    try:
        client.describe_secret(SecretId=secret_name)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            logger.error(
                f"Secret '{secret_name}' not found in AWS Secrets Manager")
            logger.error(
                "Please ensure the secret is created with the following structure:")
            logger.error("""
{
    "account": "your-account",
    "host": "your-host",
    "username": "your-username",
    "private_key": "your-private-key",
    "role": "your-role"
}
""")
            return False
        raise


def get_snowflake_session(secret_name: str) -> Session:
    """Create Snowflake session from AWS Secrets Manager credentials"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')

    try:
        secret = client.get_secret_value(SecretId=secret_name)
        creds = json.loads(secret['SecretString'])

        return Session.builder.configs({
            "account": creds['account'],
            "host": creds['host'],
            "user": creds['username'],
            "private_key": creds['private_key'],
            "role": creds['role'],
            "warehouse": "COMPUTE_WH"  # Default warehouse
        }).create()
    except Exception as e:
        logger.error(f"Failed to get Snowflake credentials: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description='Deploy Snowflake infrastructure')
    parser.add_argument('--env', choices=['dev', 'stg', 'prd'], default='dev',
                        help='Environment to deploy to')
    parser.add_argument('--fort', choices=['admin', 'medallion', 'all'], default='all',
                        help='Fort to deploy')
    args = parser.parse_args()

    # Verify secret exists
    secret_name = 'snowflake/accountadmin'
    if not verify_secret_exists(secret_name):
        return 1

    # Create Snowflake session
    try:
        session = get_snowflake_session(secret_name)
        snow = Root(session)
    except Exception as e:
        logger.error(f"Failed to initialize Snowflake session: {str(e)}")
        return 1

    # Deploy stacks
    try:
        if args.fort in ['admin', 'all']:
            logger.info(f"Deploying Admin stack to {args.env}...")
            admin = AdminFort(snow=snow, environment=args.env)
            admin.deploy()
            logger.info("Admin stack deployed successfully")

        if args.fort in ['medallion', 'all']:
            logger.info(f"Deploying Medallion stack to {args.env}...")
            medallion = MedallionFort(snow=snow, environment=args.env)
            medallion.deploy()
            logger.info("Medallion stack deployed successfully")

    except Exception as e:
        logger.error(f"Deployment failed: {str(e)}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
