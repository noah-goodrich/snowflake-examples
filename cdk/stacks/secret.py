from aws_cdk import (
    Stack,
    aws_secretsmanager as secretsmanager,
    aws_lambda as lambda_,
    Duration,
    RemovalPolicy,
)
from constructs import Construct
import os
import json
import yaml
from typing import Any, Dict


class SecretStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Load configuration
        config_path = os.path.join(os.path.dirname(
            __file__), 'config', 'secrets.yaml')
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        # Create rotation Lambda using direct asset
        rotation_lambda = lambda_.Function(self, 'SecretRotation',
                                           runtime=lambda_.Runtime.PYTHON_3_9,
                                           code=lambda_.Code.from_asset('lambdas/rotation'),
                                           handler='index.handler',
                                           timeout=Duration.minutes(5),
                                           description='Rotates secrets based on their type'
                                           )

        self.secrets = {}

        # Create secrets from configuration
        for secret_name, secret_config in config['secrets'].items():
            # Get the template
            template_name = secret_config['template']
            template = config['templates'][template_name]

            # Merge template with overrides
            secret_template = template['template'].copy()
            if 'template_overrides' in secret_config:
                secret_template.update(secret_config['template_overrides'])

            # Merge password options (template defaults with secret-specific overrides)
            password_options = template.get('password_options', {}).copy()
            if 'password_options' in secret_config:
                password_options.update(secret_config['password_options'])

            # Create properly formatted JSON template
            secret_string_template = json.dumps({
                "account": secret_template.get('account', ''),
                "user": secret_template.get('user', ''),
                "password": secret_template.get('password', '')
            })

            # Create the secret
            secret = secretsmanager.Secret(self, secret_name,
                                           secret_name=secret_name,
                                           description=secret_config['description'],
                                           generate_secret_string=secretsmanager.SecretStringGenerator(
                                               secret_string_template=secret_string_template,
                                               generate_string_key='password',
                                               password_length=password_options.get(
                                                   'password_length', 32),
                                               exclude_characters=password_options.get(
                                                   'exclude_characters', '"@/\\'),
                                               exclude_punctuation=password_options.get(
                                                   'exclude_punctuation', True),
                                               require_each_included_type=password_options.get(
                                                   'require_each_included_type', True)
                                           ),
                                           removal_policy=RemovalPolicy.RETAIN
                                           )

            # Set up rotation schedule
            secret.add_rotation_schedule('RotationSchedule',
                                         rotation_lambda=rotation_lambda,
                                         automatically_after=Duration.days(
                                             template['rotation_days'])
                                         )

            # Store reference to the secret
            self.secrets[secret_name] = secret

    def get_secret(self, secret_name: str) -> secretsmanager.Secret:
        """Returns the specified secret"""
        if secret_name not in self.secrets:
            raise ValueError(f"Secret '{secret_name}' not found")
        return self.secrets[secret_name]
