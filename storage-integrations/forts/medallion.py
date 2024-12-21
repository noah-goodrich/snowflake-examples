"""
Medallion Architecture Stack for Snowflake Data Platform

This stack implements the medallion architecture with four core databases:
- BRONZE: Raw data landing zone
- SILVER: Standardized and cleansed data
- GOLD: Business-ready aggregates and metrics
- PLATINUM: ML-ready features and model artifacts

Each database includes:
- Multiple warehouses of different sizes (X-SMALL through 3X-LARGE)
- Database roles (OWNER, RW, RO)
- Standard schemas based on purpose
"""

from typing import List
from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.warehouse import Warehouse
from snowflake.core.schema import Schema

from .fort import SnowFort


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
