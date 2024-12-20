import json
import logging
import os
import boto3
import snowflake.connector
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """Secrets Manager Rotation Template"""
    arn = event['SecretId']
    token = event['ClientRequestToken']
    step = event['Step']

    # Setup the client
    service_client = boto3.client('secretsmanager')

    # Make sure the version is staged correctly
    metadata = service_client.describe_secret(SecretId=arn)
    if not metadata['RotationEnabled']:
        logger.error("Secret %s is not enabled for rotation" % arn)
        raise ValueError("Secret %s is not enabled for rotation" % arn)

    versions = metadata['VersionIdsToStages']
    if token not in versions:
        logger.error(
            "Secret version %s has no stage for rotation of secret %s." % (token, arn))
        raise ValueError(
            "Secret version %s has no stage for rotation of secret %s." % (token, arn))

    if "AWSCURRENT" in versions[token]:
        logger.info(
            "Secret version %s already set as AWSCURRENT for secret %s." % (token, arn))
        return

    elif "AWSPENDING" not in versions[token]:
        logger.error(
            "Secret version %s not set as AWSPENDING for rotation of secret %s." % (token, arn))
        raise ValueError(
            "Secret version %s not set as AWSPENDING for rotation of secret %s." % (token, arn))

    if step == "createSecret":
        create_secret(service_client, arn, token)
    elif step == "setSecret":
        set_secret(service_client, arn, token)
    elif step == "testSecret":
        test_secret(service_client, arn, token)
    elif step == "finishSecret":
        finish_secret(service_client, arn, token)
    else:
        raise ValueError("Invalid step parameter")


def create_secret(service_client, arn, token):
    # Generate a new password
    current_dict = get_secret_dict(service_client, arn, "AWSCURRENT")
    pending_dict = current_dict.copy()
    pending_dict['password'] = generate_password()

    # Put the new secret
    service_client.put_secret_value(
        SecretId=arn,
        ClientRequestToken=token,
        SecretString=json.dumps(pending_dict),
        VersionStages=['AWSPENDING']
    )


def set_secret(service_client, arn, token):
    # Get the pending secret
    pending_dict = get_secret_dict(service_client, arn, "AWSPENDING")

    # Update password in Snowflake
    conn = get_snowflake_connection(pending_dict)
    with conn.cursor() as cursor:
        cursor.execute(
            f"ALTER USER {pending_dict['username']} SET PASSWORD = '{pending_dict['password']}'")


def test_secret(service_client, arn, token):
    # Get the pending secret and test it
    pending_dict = get_secret_dict(service_client, arn, "AWSPENDING")
    conn = get_snowflake_connection(pending_dict)
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1")


def finish_secret(service_client, arn, token):
    # Mark the secret as current
    service_client.update_secret_version_stage(
        SecretId=arn,
        VersionStage="AWSCURRENT",
        MoveToVersionId=token,
        RemoveFromVersionId=get_current_version(service_client, arn)
    )

# Helper functions


def get_secret_dict(service_client, arn, stage):
    response = service_client.get_secret_value(
        SecretId=arn,
        VersionStage=stage
    )
    return json.loads(response['SecretString'])


def get_current_version(service_client, arn):
    metadata = service_client.describe_secret(SecretId=arn)
    for version in metadata["VersionIdsToStages"]:
        if "AWSCURRENT" in metadata["VersionIdsToStages"][version]:
            return version
    return None


def get_snowflake_connection(secret_dict):
    return snowflake.connector.connect(
        user=secret_dict['username'],
        password=secret_dict['password'],
        account=secret_dict['account'],
        warehouse=secret_dict['warehouse'],
        database=secret_dict['database'],
        schema=secret_dict['schema']
    )


def generate_password():
    # Implement password generation logic that meets Snowflake requirements
    # This is a placeholder - implement proper password generation
    pass
