from aws_cdk import (
    Stack,
    CustomResource,
    custom_resources as cr,
    aws_lambda as lambda_,
    aws_iam as iam
)
from constructs import Construct


class AdminStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Lambda function that will handle Snowflake operations
        snowflake_handler = lambda_.Function(
            self, 'SnowflakeHandler',
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset('lambda/snowflake'),
            handler='index.handler',
            environment={
                'SECRET_NAME': 'snowflake/admin'
            }
        )

        # Grant Lambda permission to access Secrets Manager
        snowflake_handler.add_to_role_policy(
            iam.PolicyStatement(
                actions=['secretsmanager:GetSecretValue'],
                resources=['arn:aws:secretsmanager:*:*:secret:snowflake/*']
            )
        )

        # Create Custom Resource provider
        provider = cr.Provider(
            self, 'SnowflakeProvider',
            on_event_handler=snowflake_handler
        )

        # Define Snowflake resources using Custom Resource
        CustomResource(
            self, 'HoidRole',
            service_token=provider.service_token,
            properties={
                'ResourceType': 'Role',
                'Name': 'HOID',
                'Comment': 'Administrative role for COSMERE',
                'Grants': ['SECURITYADMIN', 'SYSADMIN']
            }
        )

        CustomResource(
            self, 'SvcHoidUser',
            service_token=provider.service_token,
            properties={
                'ResourceType': 'User',
                'Name': 'SVC_HOID',
                'Type': 'SERVICE',
                'DefaultRole': 'HOID',
                'Comment': 'Service account for administrative automation'
            }
        )
