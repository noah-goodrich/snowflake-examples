from dataclasses import dataclass
from typing import Optional, List, Tuple
import json
import boto3
from botocore.exceptions import ClientError
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
import base64

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.user import User as SnowflakeUser, Securable as UserSecurable


@dataclass
class UserConfig:
    """Configuration for user operations"""
    name: str
    default_role: Optional[str] = None
    default_warehouse: Optional[str] = None
    default_namespace: Optional[str] = None
    comment: Optional[str] = None
    password: Optional[str] = None
    rsa_public_key: Optional[str] = None
    must_change_password: bool = True
    disabled: bool = False
    prefix_with_environment: bool = True
    is_service_account: bool = False
    secret_name: Optional[str] = None

    def validate(self) -> None:
        """Validates user configuration"""
        if not self.name:
            raise ValueError("User name cannot be empty")

        if self.is_service_account:
            # Service accounts must use key authentication
            if self.password:
                raise ValueError(
                    "Service accounts cannot use password authentication")
            if not self.default_role:
                raise ValueError("Service accounts must have a default role")
        else:
            # Regular users must have either password or key
            if not (self.password or self.rsa_public_key):
                raise ValueError(
                    "Either password or RSA public key must be provided")


class User:
    """Manages Snowflake user operations"""

    def __init__(self, snow: Root, environment: str):
        self.snow = snow
        self.environment = environment

    def create_service_account(
        self,
        name: str,
        role: str,
        comment: Optional[str] = None,
        secret_name: Optional[str] = None,
        prefix_with_environment: bool = True
    ) -> Tuple[SnowflakeUser, str]:
        """Creates a new service account with key pair authentication.

        Args:
            name: Service account name
            role: Default role for the service account
            comment: Optional comment
            secret_name: AWS Secrets Manager secret name (default: snowflake/service/{name})
            prefix_with_environment: Whether to prefix name with environment

        Returns:
            Tuple of (created user, secret name)
        """
        # Generate key pair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        # Convert private key to PEM format
        private_key_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Extract public key in PEM format
        public_key = private_key.public_key()
        public_key_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )

        # Create service account config
        config = UserConfig(
            name=name,
            default_role=role,
            rsa_public_key=public_key_pem.decode('utf-8'),
            comment=comment or f"Service account for {name}",
            must_change_password=False,
            disabled=False,
            prefix_with_environment=prefix_with_environment,
            is_service_account=True,
            secret_name=secret_name or f"snowflake/service/{name.lower()}"
        )

        # Create the user
        user = self.create(config)

        # Store credentials
        self._store_service_account_credentials(
            user_name=user.name,
            private_key=private_key_pem,
            role_name=role,
            secret_name=config.secret_name
        )

        return user, config.secret_name

    def create(self, config: UserConfig, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeUser:
        """Creates a new user"""
        config.validate()
        name = self._format_name(config.name, config.prefix_with_environment)

        # Create user
        user = self.snow.users.create(
            SnowflakeUser(
                name=name,
                default_role=config.default_role,
                default_warehouse=config.default_warehouse,
                default_namespace=config.default_namespace,
                comment=config.comment,
                password=config.password,
                rsa_public_key=config.rsa_public_key,
                must_change_password=bool(config.must_change_password),
                disabled=bool(config.disabled)
            ),
            mode=mode
        )

        return user

    def _store_service_account_credentials(
        self,
        user_name: str,
        private_key: bytes,
        role_name: str,
        secret_name: str
    ) -> None:
        """Stores service account credentials in AWS Secrets Manager"""
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')

        secret_string = json.dumps({
            'username': user_name,
            'private_key': private_key.decode('utf-8'),
            'account': self.snow.session.get_current_account().replace('"', ''),
            'host': self.snow._hostname,
            'role': role_name
        })

        try:
            secret = client.get_secret_value(SecretId=secret_name)
            client.put_secret_value(
                SecretId=secret_name,
                SecretString=secret_string
            )
        except client.exceptions.ResourceNotFoundException:
            client.create_secret(
                Name=secret_name,
                SecretString=secret_string
            )

    def alter(self, name: str, config: UserConfig) -> SnowflakeUser:
        """Alters an existing user"""
        config.validate()
        user = self.get(name)
        if not user:
            raise ValueError(f"User {name} does not exist")

        # Update user properties
        return self.create(config, mode=CreateMode.or_replace)

    def drop(self, name: str) -> None:
        """Drops a user"""
        user = self.get(name)
        if user:
            user.drop()

    def get(self, name: str) -> Optional[SnowflakeUser]:
        """Gets a user by name"""
        try:
            return self.snow.users[self._format_name(name, True)]
        except KeyError:
            try:
                return self.snow.users[name.upper()]
            except KeyError:
                return None

    def grant_role(self, user_name: str, role_name: str) -> None:
        """Grants a role to a user"""
        user = self.get(user_name)
        if not user:
            raise ValueError(f"User {user_name} does not exist")

        user.grant_role(
            role_type='ROLE',
            role=UserSecurable(name=role_name)
        )

    def revoke_role(self, user_name: str, role_name: str) -> None:
        """Revokes a role from a user"""
        user = self.get(user_name)
        if not user:
            raise ValueError(f"User {user_name} does not exist")

        self.snow.session.sql(
            f"REVOKE ROLE {role_name} FROM USER {user.name}"
        ).collect()

    def enable(self, name: str) -> None:
        """Enables a user"""
        user = self.get(name)
        if user:
            self.snow.session.sql(
                f"ALTER USER {user.name} SET DISABLED = FALSE"
            ).collect()

    def disable(self, name: str) -> None:
        """Disables a user"""
        user = self.get(name)
        if user:
            self.snow.session.sql(
                f"ALTER USER {user.name} SET DISABLED = TRUE"
            ).collect()

    def reset_password(self, name: str, new_password: str) -> None:
        """Resets a user's password"""
        user = self.get(name)
        if user:
            self.snow.session.sql(
                f"ALTER USER {user.name} SET PASSWORD = '{new_password}'"
            ).collect()

    def update_rsa_public_key(self, name: str, public_key: str, key_number: int = 1) -> None:
        """Updates a user's RSA public key.

        Args:
            name: User name
            public_key: RSA public key in PEM format
            key_number: Key slot to update (1 or 2)
        """
        if key_number not in [1, 2]:
            raise ValueError("key_number must be 1 or 2")

        user = self.get(name)
        if user:
            key_param = "RSA_PUBLIC_KEY_2" if key_number == 2 else "RSA_PUBLIC_KEY"
            self.snow.session.sql(
                f"ALTER USER {user.name} SET {key_param} = '{public_key}'"
            ).collect()

    def remove_rsa_public_key(self, name: str, key_number: int = 1) -> None:
        """Removes a user's RSA public key.

        Args:
            name: User name
            key_number: Key slot to remove (1 or 2)
        """
        if key_number not in [1, 2]:
            raise ValueError("key_number must be 1 or 2")

        user = self.get(name)
        if user:
            key_param = "RSA_PUBLIC_KEY_2" if key_number == 2 else "RSA_PUBLIC_KEY"
            self.snow.session.sql(
                f"ALTER USER {user.name} UNSET {key_param}"
            ).collect()

    def verify_key_fingerprint(self, name: str, public_key: str) -> bool:
        """Verifies a public key's fingerprint matches what's stored in Snowflake.

        Args:
            name: User name
            public_key: RSA public key to verify

        Returns:
            bool: True if fingerprint matches, False otherwise
        """
        user = self.get(name)
        if not user:
            return False

        # Get fingerprint from Snowflake
        self.snow.session.sql(f"DESC USER {user.name}").collect()
        result = self.snow.session.sql(
            """
            SELECT SUBSTR(
                (SELECT "value" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                WHERE "property" = 'RSA_PUBLIC_KEY_FP'), 
                LEN('SHA256:') + 1
            )
            """
        ).collect()

        if not result:
            return False

        snowflake_fp = result[0][0]

        # Calculate fingerprint of provided key
        try:
            # Load the public key
            key_bytes = public_key.encode(
                'utf-8') if isinstance(public_key, str) else public_key
            pub_key = serialization.load_pem_public_key(key_bytes)

            # Get DER format
            der_data = pub_key.public_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )

            # Calculate SHA256
            digest = hashes.Hash(hashes.SHA256())
            digest.update(der_data)
            fingerprint = base64.b64encode(digest.finalize()).decode('utf-8')

            return fingerprint == snowflake_fp
        except Exception:
            return False

    def rotate_rsa_key(self, name: str, new_public_key: str) -> None:
        """Rotates a user's RSA public key by setting the new key in an unused slot
        and removing the old key.

        Args:
            name: User name
            new_public_key: New RSA public key in PEM format
        """
        user = self.get(name)
        if not user:
            raise ValueError(f"User {name} does not exist")

        # Describe user to find which key slots are in use
        self.snow.session.sql(f"DESC USER {user.name}").collect()
        result = self.snow.session.sql(
            """
            SELECT "property", "value" 
            FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
            WHERE "property" IN ('RSA_PUBLIC_KEY', 'RSA_PUBLIC_KEY_2')
            """
        ).collect()

        # Find unused slot
        used_slots = [row[0] for row in result]
        if 'RSA_PUBLIC_KEY' not in used_slots:
            self.update_rsa_public_key(name, new_public_key, 1)
            if 'RSA_PUBLIC_KEY_2' in used_slots:
                self.remove_rsa_public_key(name, 2)
        elif 'RSA_PUBLIC_KEY_2' not in used_slots:
            self.update_rsa_public_key(name, new_public_key, 2)
            self.remove_rsa_public_key(name, 1)
        else:
            # Both slots used, replace slot 2 and then remove slot 1
            self.update_rsa_public_key(name, new_public_key, 2)
            self.remove_rsa_public_key(name, 1)

    def _format_name(self, name: str, use_environment: bool) -> str:
        """Formats user name according to conventions"""
        if use_environment:
            return f"{self.environment}_{name}".upper()
        return name.upper()
