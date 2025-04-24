"""Key management service for handling user keys."""
from typing import Tuple, Protocol
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa as crypto_rsa
from cryptography.hazmat.backends import default_backend


class KeyAdapter(Protocol):
    """Protocol for key management adapters."""

    def create_private_key(self, user_name: str, private_key: str) -> None:
        """Store a private key for a user."""
        ...

    def get_private_key(self, user_name: str) -> str:
        """Retrieve a private key for a user."""
        ...

    def update_private_key(self, user_name: str, private_key: str) -> None:
        """Update a private key for a user."""
        ...

    def delete_private_key(self, user_name: str) -> None:
        """Delete a private key for a user."""
        ...


class SecretStorageAdapter:
    """Base class for secret storage adapters."""

    def store_secret(self, name: str, value: str) -> None:
        """Store a secret value.

        Args:
            name: The name of the secret
            value: The secret value to store
        """
        raise NotImplementedError

    def retrieve_secret(self, name: str) -> str:
        """Retrieve a secret value.

        Args:
            name: The name of the secret

        Returns:
            The secret value
        """
        raise NotImplementedError

    def delete_secret(self, name: str) -> None:
        """Delete a secret.

        Args:
            name: The name of the secret to delete
        """
        raise NotImplementedError


class AWSSecretsManagerAdapter(SecretStorageAdapter):
    """AWS Secrets Manager adapter."""

    def __init__(self, aws_client):
        """Initialize the adapter.

        Args:
            aws_client: Boto3 Secrets Manager client
        """
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


class KeyManagementService:
    """Service for managing RSA key pairs."""

    def __init__(self, storage_adapter: SecretStorageAdapter):
        """Initialize the service.

        Args:
            storage_adapter: The secret storage adapter to use
        """
        self.storage = storage_adapter

    def generate_key_pair(self, user_name: str) -> Tuple[str, str]:
        """Generate a new RSA key pair.

        Args:
            user_name: The name of the user

        Returns:
            Tuple of (public_key, private_key) in PEM format
        """
        # Generate private key
        private_key = crypto_rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # Get public key
        public_key = private_key.public_key()

        # Convert to PEM format
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')

        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

        # Store private key
        self.storage.store_secret(f"{user_name}_private_key", private_pem)

        return public_pem, private_pem

    def rotate_keys(self, user_name: str) -> Tuple[str, str]:
        """Rotate an existing RSA key pair.

        Args:
            user_name: The name of the user

        Returns:
            Tuple of (new_public_key, new_private_key) in PEM format
        """
        # Generate new key pair
        new_public_key, new_private_key = self.generate_key_pair(user_name)

        # Delete old private key
        self.storage.delete_secret(f"{user_name}_private_key")

        return new_public_key, new_private_key

    def validate_key_pair(self, user_name: str, public_key: str) -> bool:
        """Validate that a public key matches its stored private key.

        Args:
            user_name: The name of the user
            public_key: The public key to validate

        Returns:
            True if the key pair is valid, False otherwise
        """
        try:
            # Retrieve private key
            private_key_pem = self.storage.retrieve_secret(
                f"{user_name}_private_key")

            # Load keys
            private_key = serialization.load_pem_private_key(
                private_key_pem.encode('utf-8'),
                password=None,
                backend=default_backend()
            )
            public_key_obj = serialization.load_pem_public_key(
                public_key.encode('utf-8'),
                backend=default_backend()
            )

            # Verify they match
            return private_key.public_key().public_numbers() == public_key_obj.public_numbers()

        except Exception:
            return False
