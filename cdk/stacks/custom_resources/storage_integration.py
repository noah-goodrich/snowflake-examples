import json
import boto3
import snowflake.connector
import os
from typing import Any, Dict, Optional
import logging
from botocore.exceptions import ClientError
from snowflake.connector.errors import ProgrammingError, DatabaseError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class StorageIntegrationError(Exception):
    """Custom exception for storage integration errors"""
    pass


def validate_props(props: Dict[str, Any]) -> None:
    """Validate the properties passed to the custom resource"""
    required_props = ['IntegrationName', 'S3Bucket', 'AllowedLocations']
    missing_props = [prop for prop in required_props if prop not in props]
    if missing_props:
        raise StorageIntegrationError(
            f"Missing required properties: {', '.join(missing_props)}")

    # Validate integration name format
    if not props['IntegrationName'].isalnum() and '_' not in props['IntegrationName']:
        raise StorageIntegrationError(
            "Integration name must contain only alphanumeric characters and underscores")

    # Validate S3 locations
    for location in props['AllowedLocations']:
        if not location.startswith('s3://'):
            raise StorageIntegrationError(
                f"Invalid S3 location format: {location}")


def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    """Create and test Snowflake connection"""
    try:
        required_env_vars = ['SNOWFLAKE_USER',
                             'SNOWFLAKE_PASSWORD', 'SNOWFLAKE_ACCOUNT']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise StorageIntegrationError(
                f"Missing environment variables: {', '.join(missing_vars)}")

        conn = snowflake.connector.connect(
            user=os.environ['SNOWFLAKE_USER'],
            password=os.environ['SNOWFLAKE_PASSWORD'],
            account=os.environ['SNOWFLAKE_ACCOUNT']
        )
        # Test connection
        conn.cursor().execute('SELECT 1')
        return conn
    except Exception as e:
        raise StorageIntegrationError(
            f"Failed to connect to Snowflake: {str(e)}")


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle storage integration creation and updates"""
    logger.info(
        f"Processing {event['RequestType']} request for {event['ResourceProperties'].get('IntegrationName')}")

    try:
        props = event['ResourceProperties']
        validate_props(props)
        integration_name = props['IntegrationName']

        # Connect to Snowflake
        ctx = get_snowflake_connection()

        try:
            if event['RequestType'] in ['Create', 'Update']:
                logger.info(
                    f"Creating/Updating storage integration: {integration_name}")
                # Create/Update storage integration in Snowflake
                integration = create_or_update_integration(ctx, props)

                # Get integration properties
                desc = describe_integration(ctx, integration_name)

                # Update IAM role trust relationship
                update_iam_role(
                    props['IntegrationName'],
                    desc['STORAGE_AWS_IAM_USER_ARN'],
                    desc['STORAGE_AWS_EXTERNAL_ID']
                )

                # Update integration with role ARN
                update_integration_role(
                    ctx, integration_name, desc['STORAGE_AWS_ROLE_ARN'])

                return {
                    'PhysicalResourceId': integration_name,
                    'Data': {
                        'IntegrationName': integration_name,
                        'UserArn': desc['STORAGE_AWS_IAM_USER_ARN'],
                        'ExternalId': desc['STORAGE_AWS_EXTERNAL_ID'],
                        'RoleArn': desc['STORAGE_AWS_ROLE_ARN']
                    }
                }

            elif event['RequestType'] == 'Delete':
                # Drop integration
                drop_integration(ctx, integration_name)
                return {
                    'PhysicalResourceId': integration_name
                }

        finally:
            ctx.close()

    except Exception as e:
        logger.error(
            f"Error processing {event['RequestType']} request for {integration_name}: {str(e)}")
        raise StorageIntegrationError(
            f"Error processing {event['RequestType']} request for {integration_name}: {str(e)}")


def create_or_update_integration(ctx: Any, props: Dict[str, Any]) -> None:
    """Create or update Snowflake storage integration"""
    allowed_locations = ', '.join(
        [f"'{loc}'" for loc in props['AllowedLocations']])
    blocked_locations = ', '.join(
        [f"'{loc}'" for loc in props.get('BlockedLocations', [])])

    sql = f"""
    CREATE OR REPLACE STORAGE INTEGRATION {props['IntegrationName']}
        TYPE = EXTERNAL_STAGE
        STORAGE_PROVIDER = S3
        ENABLED = TRUE
        STORAGE_AWS_ROLE_ARN = 'placeholder'
        STORAGE_ALLOWED_LOCATIONS = ({allowed_locations})
        {f'STORAGE_BLOCKED_LOCATIONS = ({blocked_locations})' if blocked_locations else ''}
        COMMENT = '{props.get('Comment', '')}'
    """

    ctx.cursor().execute(sql)


def describe_integration(ctx: Any, integration_name: str) -> Dict[str, str]:
    """Get storage integration properties"""
    cursor = ctx.cursor()
    cursor.execute(f"DESC STORAGE INTEGRATION {integration_name}")
    return {row[0]: row[1] for row in cursor.fetchall()}


def update_iam_role(role_name: str, user_arn: str, external_id: str) -> None:
    """Update IAM role trust relationship"""
    iam = boto3.client('iam')

    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {
                "AWS": user_arn
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": external_id
                }
            }
        }]
    }

    iam.update_assume_role_policy(
        RoleName=role_name,
        PolicyDocument=json.dumps(trust_policy)
    )


def update_integration_role(ctx: Any, integration_name: str, role_arn: str) -> None:
    """Update storage integration with IAM role ARN"""
    ctx.cursor().execute(f"""
    ALTER STORAGE INTEGRATION {integration_name}
        SET STORAGE_AWS_ROLE_ARN = '{role_arn}'
    """)


def drop_integration(ctx: Any, integration_name: str) -> None:
    """Drop storage integration"""
    ctx.cursor().execute(
        f"DROP STORAGE INTEGRATION IF EXISTS {integration_name}")
