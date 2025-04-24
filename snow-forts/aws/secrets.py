"""AWS Secrets Manager wrapper for consistent secret management."""

from typing import Any, Dict, Optional
import boto3
from botocore.exceptions import ClientError


class SecretsManager:
    """Wrapper for AWS Secrets Manager operations."""

    def __init__(self, session: Optional[boto3.Session] = None):
        """Initialize SecretsManager with optional boto3 session.

        Args:
            session: boto3.Session instance. If not provided, creates a default session.
        """
        self.session = session or boto3.Session()
        self.client = self.session.client(service_name='secretsmanager')

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager.

        Args:
            secret_name: Name of the secret to retrieve

        Returns:
            Dict containing the secret value

        Raises:
            ValueError: If secret not found
            ClientError: For other AWS errors
        """
        try:
            return self.client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise ValueError(f"Secret {secret_name} not found")
            raise
