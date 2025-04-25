"""
Admin Stack for Snowflake Infrastructure Management

This stack handles the creation and configuration of core administrative components
in Snowflake, including:
- HOID administrative role creation 
- SVC_HOID service account setup with RSA key pair authentication
- AWS Secrets Manager integration for credential management
- Admin warehouse (COSMERE_XSMALL) provisioning
- Admin database (COSMERE) and schema creation

The stack implements security best practices including:
- Key pair verification using SHA256 fingerprint comparison
- Least privilege access through role assignments
- Automated secret creation
- RSA key pair authentication for enhanced security

Key pair authentication implementation follows Snowflake's recommended practices:
- Uses PKCS#8 format for private keys
- Implements fingerprint verification
- Supports key rotation capabilities
- Stores credentials securely in AWS Secrets Manager

References:
- Snowflake Key Pair Authentication Guide: https://docs.snowflake.com/en/user-guide/key-pair-auth
- SELECT Developer Guide for Key Pair Setup: https://select.dev/docs/snowflake-developer-guide/snowflake-key-pair

Dependencies:
    - boto3: AWS SDK for Secrets Manager interaction
    - snowflake.core: Core Snowflake infrastructure management
    - snowflake.snowpark: Snowflake session management
    - cryptography: RSA key pair generation and management
"""

import boto3
import json
from typing import Any, Dict, Optional
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import base64

from snowflake.core import Root
from snowflake.snowpark import Session

from .snow import SnowFort
from specs.warehouse import WarehouseSpec
from specs.database import DatabaseSpec
from specs.role import RoleSpec
from specs.user import UserSpec
from state_managers.types import StateChangeMetadata
from state_managers.warehouse import WarehouseStateManager
from state_managers.database import DatabaseStateManager
from state_managers.role import RoleStateManager
from state_managers.user import UserStateManager
from aws.secrets import SecretsManager


class AdminFort(SnowFort):
    """Handles core Snowflake administrative setup"""

    def deploy(self):
        """Deploys the complete admin setup"""
        # Setup admin role
        role_spec = RoleSpec(
            name='HOID',
            comment='Administrative role for COSMERE',
            granted_roles=['SECURITYADMIN', 'SYSADMIN'],
            prefix_with_environment=False
        )
        self.role_state.apply(role_spec)

        # Create service account with key pair
        user_spec = UserSpec(
            name='SVC_HOID',
            role='HOID',
            comment='Service account for administrative automation',
            secret_name='snowflake/admin',
            prefix_with_environment=False
        )
        self.user_state.apply(user_spec)

        # Define and apply warehouse state
        warehouse_spec = WarehouseSpec(
            name='COSMERE_XS',
            size='XSMALL',
            auto_suspend=1,
            auto_resume=True,
            prefix_with_environment=False
        )
        self.warehouse_state.apply(warehouse_spec)

        # Define and apply database state
        database_spec = DatabaseSpec(
            name='COSMERE',
            schemas=['LOGS', 'AUDIT', 'ADMIN', 'SECURITY'],
            comment='Administrative database for platform management',
            prefix_with_environment=False
        )
        self.database_state.apply(database_spec)

        # Grant COSMERE_OWNER role to HOID through role state manager
        role_grant_spec = RoleSpec(
            name='COSMERE_OWNER',
            granted_to=['HOID'],
            prefix_with_environment=False
        )
        self.role_state.apply(role_grant_spec)

    def _create_session(self) -> Root:
        """Creates a new Snowflake session using stored credentials"""
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        secret = client.get_secret_value(SecretId='snowflake/admin')
        secret_value = json.loads(secret['SecretString'])

        # Create session config
        session_config = {
            "account": secret_value['account'],
            "host": secret_value['host'],
            "user": secret_value['username'],
            "private_key": secret_value['private_key'],
            "role": secret_value['role'],
            "warehouse": "COMPUTE_WH"  # Default warehouse
        }

        # Create and return session
        snowpark_session = Session.builder.configs(session_config).create()
        return Root(snowpark_session)
