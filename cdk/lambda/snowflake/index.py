import boto3
import json
from snowflake.snowpark import Session
from snowflake.core import Root


def handler(event, context):
    # Get Snowflake credentials from Secrets Manager
    secret_name = event['ResourceProperties'].get(
        'SecretName', 'snowflake/admin')
    client = boto3.client('secretsmanager')
    secret = json.loads(client.get_secret_value(
        SecretId=secret_name)['SecretString'])

    # Create Snowflake session
    session = Session.builder.configs({
        "account": secret['account'],
        "host": secret['host'],
        "user": secret['username'],
        "password": secret['password'],
        "role": secret['role'],
        "warehouse": 'COMPUTE_WH'
    }).create()

    snow = Root(session)

    # Handle resource creation/update/deletion based on event type
    if event['RequestType'] in ['Create', 'Update']:
        if event['ResourceProperties']['ResourceType'] == 'Role':
            create_role(snow, event['ResourceProperties'])
        elif event['ResourceProperties']['ResourceType'] == 'User':
            create_user(snow, event['ResourceProperties'])
    elif event['RequestType'] == 'Delete':
        # Handle resource deletion
        pass

    return {
        'PhysicalResourceId': f"snowflake-{event['ResourceProperties']['Name']}"
    }
