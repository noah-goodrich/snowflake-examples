"""AWS Secrets Manager adapter for key management."""
from typing import Any
from services.key_management import SecretStorageAdapter


class AWSSecretsManagerAdapter(SecretStorageAdapter):
    """AWS Secrets Manager adapter for storing secrets."""

    def __init__(self, aws_client: Any):
        """Initialize the adapter with an AWS Secrets Manager client."""
        self.client = aws_client

    def store_secret(self, name: str, value: str) -> None:
        """Store a secret in AWS Secrets Manager.

        Args:
            name: The name of the secret
            value: The secret value to store
        """
        try:
            self.client.create_secret(
                Name=name,
                SecretString=value
            )
        except self.client.exceptions.ResourceExistsException:
            self.client.update_secret(
                SecretId=name,
                SecretString=value
            )

    def retrieve_secret(self, name: str) -> str:
        """Retrieve a secret from AWS Secrets Manager.

        Args:
            name: The name of the secret

        Returns:
            The secret value

        Raises:
            Exception: If the secret is not found
        """
        try:
            response = self.client.get_secret_value(SecretId=name)
            return response['SecretString']
        except self.client.exceptions.ResourceNotFoundException:
            raise Exception(f"Secret {name} not found")

    def delete_secret(self, name: str) -> None:
        """Delete a secret from AWS Secrets Manager.

        Args:
            name: The name of the secret to delete
        """
        try:
            self.client.delete_secret(
                SecretId=name,
                ForceDeleteWithoutRecovery=True
            )
        except self.client.exceptions.ResourceNotFoundException:
            pass
