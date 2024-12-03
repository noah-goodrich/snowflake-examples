from aws_cdk import (
    Stack,
    aws_iam as iam,
    CustomResource,
    custom_resources as cr,
    aws_lambda as lambda_
)
from constructs import Construct
import yaml
from typing import Any, Dict
import os


class StorageIntegrationStack(Stack):
    def __init__(self, scope: Construct, id: str, snowflake_secret: Dict[str, Any], **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Load configuration
        with open(os.path.join(os.path.dirname(__file__), 'config', 'storage.yaml'), 'r') as file:
            config = yaml.safe_load(file)

        # Create Lambda function for custom resource
        handler = lambda_.Function(self, 'StorageIntegrationHandler',
                                   runtime=lambda_.Runtime.PYTHON_3_9,
                                   code=lambda_.Code.from_asset(
                                       'stacks/cross_platform/custom_resources'),
                                   handler='storage_integration.handler',
                                   environment={
                                       'SNOWFLAKE_ACCOUNT': snowflake_secret['account'],
                                       'SNOWFLAKE_USER': snowflake_secret['user'],
                                       'SNOWFLAKE_PASSWORD': snowflake_secret['password']
                                   })

        # Create provider
        provider = cr.Provider(self, 'StorageIntegrationProvider',
                               on_event_handler=handler)

        # For each storage integration in config
        for integration_name, integration_config in config['storage_integrations'].items():
            # Create custom resource to handle Snowflake-AWS orchestration
            integration = CustomResource(self, f'StorageIntegration-{integration_name}',
                                         service_token=provider.service_token,
                                         properties={
                                             'IntegrationName': integration_name,
                                             'S3Bucket': integration_config['s3_bucket'],
                                             'AllowedLocations': integration_config['storage_allowed_locations'],
                                             'BlockedLocations': integration_config.get('storage_blocked_locations', []),
                                             'Comment': integration_config.get('comment', '')
                                         })

            # Create IAM role (will be updated with proper trust relationship by Lambda)
            role = iam.Role(self, f'SnowflakeRole-{integration_name}',
                            assumed_by=iam.ServicePrincipal(
                                's3.amazonaws.com'),  # Temporary principal
                            role_name=integration_config['aws_role'])

            # Add S3 permissions
            role.add_to_policy(iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    's3:GetObject',
                    's3:GetObjectVersion',
                    's3:ListBucket',
                    's3:GetBucketLocation'
                ],
                resources=[
                    f"arn:aws:s3:::{integration_config['s3_bucket']}",
                    f"arn:aws:s3:::{integration_config['s3_bucket']}/*"
                ]
            ))
