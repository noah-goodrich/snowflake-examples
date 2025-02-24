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
from typing import Any, Dict
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import base64

from snowflake.core import Root
from snowflake.snowpark import Session

from .fort import SnowFort
from resources.warehouse import Warehouse, WarehouseConfig
from resources.database import Database, DatabaseConfig
from resources.role import Role, RoleConfig
from resources.user import User, UserConfig


class AdminFort(SnowFort):
    """Handles core Snowflake administrative setup"""

    def __init__(self, snow: Root, environment: str):
        super().__init__(snow, environment)
        self.warehouse_manager = Warehouse(snow, environment)
        self.database_manager = Database(snow, environment)
        self.role_manager = Role(snow, environment)
        self.user_manager = User(snow, environment)

    def deploy(self):
        """Deploys the complete admin setup"""
        # Setup admin role
        hoid = self.role_manager.create(RoleConfig(
            name='HOID',
            comment='Administrative role for COSMERE',
            granted_roles=['SECURITYADMIN', 'SYSADMIN'],
            prefix_with_environment=False
        ))

        # Create service account with key pair
        user, private_key = self.user_manager.create_service_account(
            name='SVC_HOID',
            role='HOID',
            comment='Service account for administrative automation',
            secret_name='snowflake/admin',
            prefix_with_environment=False
        )

        # Create new session with service account
        self.snow = self._create_session()

        # Reinitialize managers with new session
        self.warehouse_manager = Warehouse(self.snow, self.environment)
        self.database_manager = Database(self.snow, self.environment)
        self.role_manager = Role(self.snow, self.environment)
        self.user_manager = User(self.snow, self.environment)

        # Create admin warehouse
        self.warehouse_manager.create(WarehouseConfig(
            name='COSMERE_XS',
            size='XSMALL',
            auto_suspend=1,
            auto_resume=True,
            prefix_with_environment=False
        ))

        # Create admin database with schemas
        self.database_manager.create(DatabaseConfig(
            name='COSMERE',
            schemas=['LOGS', 'AUDIT', 'ADMIN', 'SECURITY'],
            comment='Administrative database for platform management',
            prefix_with_environment=False
        ))

        # Grant COSMERE_OWNER role to HOID
        self.role_manager.grant_role('HOID', 'COSMERE_OWNER')

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
