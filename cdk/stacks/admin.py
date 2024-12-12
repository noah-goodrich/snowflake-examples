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

import base64
import boto3
import hashlib
import json
from cryptography.hazmat.primitives import serialization
from typing import Any, Dict

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.warehouse import Warehouse
from snowflake.core.schema import Schema
from snowflake.core.role import Role, Securable as RoleSecurable, ContainingScope as RoleContainingScope
from snowflake.core.user import User, Securable as UserSecurable
from snowflake.snowpark import Session

from .snow import SnowStack
from ..libs.crypt import Crypt


class Admin(SnowStack):
    def deploy(self):
        hoid = self.snow.roles.create(role=Role(
            name='HOID',
            comment='Administrative role for COSMERE'
        ), mode=CreateMode.if_not_exists)

        hoid.grant_role(role_type='ROLE', role=RoleSecurable(
            name='SECURITYADMIN'
        ))
        hoid.grant_role(role_type='ROLE', role=RoleSecurable(
            name='SYSADMIN'
        ))

        private_key, public_key = Crypt.generate_asymmetrical_keys()

        user = self.snow.users.create(user=User(
            name='SVC_HOID',
            type='SERVICE',
            default_role='HOID',
            rsa_public_key=public_key.decode('utf-8'),
            comment='Service account for administrative automation'
        ), mode=CreateMode.if_not_exists)

        user.grant_role(role_type='ROLE', role=UserSecurable(
            name='HOID'
        ))

        # Add verification step
        result = self.snow.session.sql(f"DESC USER {user.name}").collect()
        stored_fingerprint = None
        for row in result:
            if row['property'] == 'RSA_PUBLIC_KEY_FP':
                stored_fingerprint = row['value'].replace('SHA256:', '')
                break

        # Generate fingerprint from our public key using openssl equivalent
        # openssl rsa -pubin -in public_key.pem -outform DER | openssl dgst -sha256 -binary | openssl enc -base64
        public_key_der = serialization.load_pem_public_key(public_key).public_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        generated_fingerprint = hashlib.sha256(public_key_der).digest()
        generated_fingerprint = base64.b64encode(
            generated_fingerprint).decode('utf-8')

        if stored_fingerprint != generated_fingerprint:
            raise ValueError(
                "Generated key fingerprint doesn't match stored fingerprint in Snowflake")

        hoid.grant_privileges(['OWNERSHIP'], 'USER', securable=RoleSecurable(
            name='SVC_HOID'
        ))

        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')

        try:
            secret = client.get_secret_value(SecretId='snowflake/admin')
        except client.exceptions.ResourceNotFoundException:
            secret = None

        secret_string = json.dumps({
            'username': user.name,
            'private_key': private_key.decode('utf-8'),
            'account': self.snow.session.get_current_account().replace('"', ''),
            'host': self.snow._hostname,
            'role': 'HOID'
        })

        if secret is None:
            client.create_secret(
                Name='snowflake/admin',
                SecretString=secret_string
            )
        else:
            client.put_secret_value(
                SecretId='snowflake/admin',
                SecretString=secret_string
            )

        # Get the secret for Snowpark session creation
        secret = client.get_secret_value(SecretId='snowflake/admin')
        secret_value = json.loads(secret['SecretString'])

        # This returns an RSAPrivateKey object
        private_key = Crypt.load_private_key(secret_value['private_key'])

        session = Session.builder.configs({
            "account": secret_value['account'],
            "host": secret_value['host'],
            "user": secret_value['username'],
            "private_key": private_key,
            "role": secret_value['role'],
            # default to COMPUTE_WH if not specified
            "warehouse": "COMPUTE_WH"
        }).create()

        self.snow = Root(session)

        # Create admin warehouse
        self.snow.warehouses.create(Warehouse(
            name='COSMERE_XS',
            warehouse_size='XSMALL',
            auto_suspend=1,
            auto_resume='true'
            # initially_suspended='true' # TODO: fix this. Don't know why it's not working. --ndg 12/12/2024
        ), mode=CreateMode.if_not_exists)

        # Create COSMERE database
        self.create_if_not_exists_database(
            name='COSMERE',
            description="Administrative database for platform management",
            prefix_with_environment=False
        )

        hoid.grant_role(role_type='ROLE', role=RoleSecurable(
            name='COSMERE_OWNER'
        ))

        # Create schemas
        for schema_name in ['LOGS', 'AUDIT', 'ADMIN', 'SECURITY']:
            self.snow.databases['COSMERE'].schemas.create(Schema(
                name=schema_name, comment=f'{schema_name} schema'), mode=CreateMode.if_not_exists)
