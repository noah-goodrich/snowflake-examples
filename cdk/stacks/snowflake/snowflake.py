from aws_cdk import (
    Stack,
    aws_secretsmanager as secretsmanager,
    custom_resources as cr,
)
from constructs import Construct
import yaml
import os


class SnowflakeStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        secrets: dict[str, secretsmanager.Secret],
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Get Snowflake credentials secret
        self.snowflake_credentials = secrets['snowflake']

        # Create a Custom Resource to create Snowflake service account
        snowflake_setup = cr.AwsCustomResource(
            self,
            "SnowflakeServiceAccountSetup",
            on_create=cr.AwsSdkCall(
                service="SecretsManager",
                action="getSecretValue",
                parameters={
                    "SecretId": self.snowflake_credentials.secret_arn
                },
                physical_resource_id=cr.PhysicalResourceId.of("SnowflakeSetup")
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=[self.snowflake_credentials.secret_arn]
            )
        )

        # Get the secret value including the generated password
        secret_value = snowflake_setup.get_response_field('SecretString')

        # Now you can use this to create the Snowflake service account
        # We'll need to implement the actual Snowflake account creation logic here
        config_dir = os.path.join(os.path.dirname(__file__), "config")

        with open(os.path.join(config_dir, "admin.yaml"), 'r') as f:
            self.admin_config = yaml.safe_load(f)

        with open(os.path.join(config_dir, "databases.yaml"), 'r') as f:
            self.db_config = yaml.safe_load(f)

        with open(os.path.join(config_dir, "access_roles.yaml"), 'r') as f:
            self.access_role_config = yaml.safe_load(f)

        with open(os.path.join(config_dir, "functional_roles.yaml"), 'r') as f:
            self.fu
