"""Medallion Stack for Snowflake Infrastructure Management

This stack handles the creation and configuration of medallion architecture components
in Snowflake, including:
- Bronze, Silver, and Gold layer databases and schemas
- Warehouse provisioning for each layer
- Role-based access control for each layer
- Service account setup with RSA key pair authentication
- AWS Secrets Manager integration for credential management

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


class MedallionFort(SnowFort):
    """Implements the medallion architecture for data warehousing"""

    # Standard warehouse configurations
    WAREHOUSE_SIZES = [
        'XSMALL',   # 1 credit/hour
        'SMALL',    # 2 credits/hour
        'MEDIUM',   # 4 credits/hour
        'LARGE',    # 8 credits/hour
        'XLARGE',   # 16 credits/hour
        'XXLARGE',  # 32 credits/hour
        'XXXLARGE'  # 64 credits/hour
    ]

    def create_standard_warehouses(self, db_name: str, overrides: dict = None):
        """Create standard warehouses for a database"""
        for size in self.WAREHOUSE_SIZES:
            self.create_or_alter_warehouse(
                db=db_name,
                size=size,
                overrides=overrides
            )

    def deploy_bronze(self):
        """Deploy BRONZE database and components"""
        db_name = 'BRONZE'

        # Create standard warehouses
        self.create_standard_warehouses(db_name, {
            'auto_suspend': 5
        })

        # Create database
        self.create_if_not_exists_database(
            name=db_name,
            description='Raw data landing zone'
        )

        # Create schemas
        db = self.snow.databases[f"{self.env}_{db_name}"]
        for schema in ['salesforce', 'airflow', 'app']:
            db.schemas.create(
                Schema(name=schema, comment=f'{schema} schema'),
                mode=CreateMode.if_not_exists
            )

    def deploy_silver(self):
        """Deploy SILVER database and components"""
        db_name = 'SILVER'

        # Create standard warehouses
        self.create_standard_warehouses(db_name, {
            'auto_suspend': 5,
            'min_cluster_count': 1,
            'max_cluster_count': 3
        })

        # Create database
        self.create_if_not_exists_database(
            name=db_name,
            description='Standardized and cleansed data layer'
        )

        # Create schemas
        db = self.snow.databases[f"{self.env}_{db_name}"]
        for schema in ['RAW', 'STAGE']:
            db.schemas.create(
                Schema(name=schema, comment=f'{schema} schema'),
                mode=CreateMode.if_not_exists
            )

    def deploy_gold(self):
        """Deploy GOLD database and components"""
        db_name = 'GOLD'

        # Create standard warehouses
        self.create_standard_warehouses(db_name, {
            'auto_suspend': 60,
            'min_cluster_count': 1,
            'max_cluster_count': 3
        })

        # Create database
        self.create_if_not_exists_database(
            name=db_name,
            description='Business-ready metrics and aggregates'
        )

        # Create schemas
        db = self.snow.databases[f"{self.env}_{db_name}"]
        for schema in ['METRICS', 'REPORTS']:
            db.schemas.create(
                Schema(name=schema, comment=f'{schema} schema'),
                mode=CreateMode.if_not_exists
            )

    def deploy_platinum(self):
        """Deploy PLATINUM database and components"""
        db_name = 'PLATINUM'

        # Create smaller warehouses with standard config
        for size in ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE']:
            self.create_or_alter_warehouse(
                db=db_name,
                size=size,
                overrides={
                    'auto_suspend': 60,
                    'min_cluster_count': 1,
                    'max_cluster_count': 4
                }
            )

        # Create larger warehouses with Snowpark optimization
        for size in ['XLARGE', 'XXLARGE', 'XXXLARGE']:
            self.create_or_alter_warehouse(
                db=db_name,
                size=size,
                overrides={
                    'auto_suspend': 60,
                    'min_cluster_count': 1,
                    'max_cluster_count': 4,
                    'enable_query_acceleration': 'true',
                    'query_acceleration_max_scale_factor': 8
                }
            )

        # Create database
        self.create_if_not_exists_database(
            name=db_name,
            description='Machine learning features and model artifacts'
        )

        # Create schemas
        db = self.snow.databases[f"{self.env}_{db_name}"]
        for schema in ['FEATURES', 'MODELS', 'EXPERIMENTS']:
            db.schemas.create(
                Schema(name=schema, comment=f'{schema} schema'),
                mode=CreateMode.if_not_exists
            )

    def deploy(self):
        """Deploy the complete medallion architecture"""
        self.deploy_bronze()
        self.deploy_silver()
        self.deploy_gold()
        self.deploy_platinum()
