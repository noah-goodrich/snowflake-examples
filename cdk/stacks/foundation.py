import logging
import boto3
import json
from aws_cdk import Stack
from constructs import Construct

from snowflake.snowpark import Session
from snowflake.core import Root

from stacks.admin import Admin
from stacks.medallion import Medallion

"""
Logging setup
"""
logger = logging.getLogger('foundation')
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class Foundation(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        secret_name = "snowflake/accountadmin"
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')

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

        logger.info(f"Snowflake connection established")

        # Create admin stack
        admin = Admin(self, "Admin", snow=snow)
        snow = admin.deploy()

        logger.info(f"Admin stack deployed")
        # Create medallion stack
        medallion = Medallion(self, "Medallion", snow=snow)
        medallion.deploy()

        logger.info(f"Medallion stack deployed")
