from aws_cdk import (
    Stack,
    aws_secretsmanager as secretsmanager,
    aws_lambda as lambda_,
    Duration,
    CfnOutput,
)
from constructs import Construct
import subprocess
import os
import yaml
import json


class SecretStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Load secrets configuration
        config_path = os.path.join(os.path.dirname(
            __file__), "..", "config", "secrets.yaml")
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Build Lambda package for rotation
        lambda_dir = os.path.join(os.path.dirname(
            __file__), "..", "lambda", "rotation")
        subprocess.check_call(["python", "build.py"], cwd=lambda_dir)

        # Create secrets dictionary to store all secrets
        self.secrets = {}

        # Create secrets based on configuration
        for secret_id, secret_config in self.config['secrets'].items():
            self._create_secret(secret_id, secret_config)

    def _create_secret(self, secret_id: str, config: dict):
        """Create a secret based on configuration"""

        # Create the secret
        secret = secretsmanager.Secret(
            self,
            config['name'],
            description=config['description'],
            secret_string_generator=secretsmanager.SecretStringGenerator(
                secret_string_template=self._format_template(
                    config['template']),
                password_length=config['password_options']['length'],
                exclude_characters=config['password_options']['exclude_chars'],
                exclude_punctuation=config['password_options']['exclude_punctuation'],
                include_space=config['password_options']['include_space'],
                generate_string_key="password"
            )
        )

        # Store in our secrets dictionary using the colloquial name
        self.secrets[secret_id] = secret

        # Setup rotation if enabled
        if config['rotation']['enabled']:
            rotation_lambda = self._create_rotation_lambda(secret_id, secret)
            secret.grant_rotation(rotation_lambda)
            secret.add_rotation_schedule(
                f"{config['name']}RotationSchedule",
                rotation_lambda=rotation_lambda,
                automatically_after=Duration.days(
                    config['rotation']['interval_days'])
            )

        # Create output
        CfnOutput(
            self,
            f"{secret_id.title().replace('_', '')}SecretArn",
            value=secret.secret_arn,
            description=f"ARN of {config['description']}"
        )

    def _format_template(self, template: dict) -> str:
        """Convert template dictionary to JSON string"""
        return json.dumps(template)

    def _create_rotation_lambda(self, secret_id: str, secret: secretsmanager.Secret) -> lambda_.Function:
        """Create rotation Lambda for a secret"""
        return lambda_.Function(
            self,
            f"{secret_id.title().replace('_', '')}RotationLambda",
            runtime=lambda_.Runtime.PYTHON_3_9,
            handler="index.handler",
            code=lambda_.Code.from_asset(
                os.path.join(
                    os.path.dirname(__file__),
                    "..",
                    "lambda",
                    "rotation",
                    "dist",
                    "rotation-lambda.zip"
                )
            ),
            timeout=Duration.minutes(5),
            environment={
                "SECRET_ARN": secret.secret_arn
            }
        )
