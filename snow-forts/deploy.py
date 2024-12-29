import argparse
import boto3
import json
from snowflake.snowpark import Session
from snowflake.core import Root

from forts.admin import AdminFort
from forts.medallion import MedallionFort


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
        raise Exception(f"Failed to get Snowflake credentials: {str(e)}")


def main():
    parser = argparse.ArgumentParser(
        description='Deploy Snowflake infrastructure')
    parser.add_argument('--env', choices=['dev', 'stg', 'prd'], default='dev',
                        help='Environment to deploy to')
    parser.add_argument('--fort', choices=['admin', 'medallion', 'all'], default='all',
                        help='Fort to deploy')
    args = parser.parse_args()

    # Create Snowflake session
    try:
        session = get_snowflake_session('snowflake/accountadmin')
        snow = Root(session)
    except Exception as e:
        print(f"Failed to initialize Snowflake session: {str(e)}")
        return 1

    # Deploy stacks
    try:
        if args.fort in ['admin', 'all']:
            print(f"Deploying Admin stack to {args.env}...")
            admin = AdminFort(snow=snow, environment=args.env)
            admin.deploy()
            print("Admin stack deployed successfully")

        if args.fort in ['medallion', 'all']:
            print(f"Deploying Medallion stack to {args.env}...")
            medallion = MedallionFort(snow=snow, environment=args.env)
            medallion.deploy()
            print("Medallion stack deployed successfully")

    except Exception as e:
        print(f"Deployment failed: {str(e)}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
