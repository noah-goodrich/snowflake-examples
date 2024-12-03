from constructs import Construct
from typing import Any, Dict
import json
from snowflake.snowpark import Session
from snowflake.core import Root
from .admin import AdminStack
from .roles import RoleStack
from .databases import DatabaseStack
from .warehouses import WarehouseStack
from stacks.snow_stack import SnowStack


class SnowflakeStack(SnowStack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Just initialize basic properties
        self.snow = None
        self.admin_config = self.load_config(
            'stacks/snowflake/config/admin.yaml')
        self.admin_role = self.admin_config['admin_role']
        # Service account credentials for SVC_HOID, populated after admin stack deployment
        # Used to store username/password for reconnecting with proper admin role
        self.svc_admin = None

    def deploy(self):
        """Main deployment logic"""
        try:
            # Get credentials
            accountadmin_secret = self._get_accountadmin_secret()
            svc_admin_secret = self._get_svc_admin_secret()

            # Initialize and deploy components
            self._deploy_admin(accountadmin_secret, svc_admin_secret)
            self._deploy_databases()
            self._deploy_roles()
            self._deploy_warehouses()

        except Exception as e:
            if self.snow:
                self.snow.close()
            raise Exception(f"Failed to initialize Snowflake stack: {e}")

    def _deploy_admin(self, accountadmin_secret, svc_admin_secret):
        """Deploy admin resources"""
        # Create initial session as ACCOUNTADMIN
        session = Session.builder.configs({
            "account": accountadmin_secret['account'],
            "user": accountadmin_secret['username'],
            "password": accountadmin_secret['password'],
            "role": "ACCOUNTADMIN",
            "warehouse": "COMPUTE_WH",  # Default warehouse
            "database": "SNOWFLAKE",    # System database
            "schema": "PUBLIC"          # Default schema
        }).create()

        self.snow = Root(session)

        admin_stack = AdminStack(
            self,
            "Admin",
            snow=self.snow,
            admin_config=self.admin_config,
            admin_role=self.admin_role,
            svc_admin_secret=svc_admin_secret
        )
        self.svc_admin = admin_stack.svc_admin

        # Reconnect as SVC_ADMIN
        session.close()
        session = Session.builder.configs({
            "account": svc_admin_secret['account'],
            "user": self.svc_admin['name'],
            "password": self.svc_admin['password'],
            "role": self.admin_role['name'],
            "warehouse": "COMPUTE_WH",
            "database": "SNOWFLAKE",
            "schema": "PUBLIC"
        }).create()

        self.snow = Root(session)

    def _get_accountadmin_secret(self) -> Dict[str, str]:
        """Get ACCOUNTADMIN credentials from Secrets Manager"""
        try:
            secret = self._get_secret("snowflake/accountadmin")
            return json.loads(secret['SecretString'])
        except Exception as e:
            raise Exception(f"Failed to get ACCOUNTADMIN secret: {e}")

    def _get_svc_admin_secret(self) -> Dict[str, str]:
        """Get SVC_ADMIN credentials from Secrets Manager"""
        try:
            secret = self.get_secret("snowflake/svc-admin")
            return json.loads(secret['SecretString'])
        except Exception as e:
            raise Exception(f"Failed to get SVC_ADMIN secret: {e}")

    def _deploy_databases(self):
        """Deploy database resources"""
        database_stack = DatabaseStack(
            self,
            "Databases",
            snow=self.snow,
            admin_config=self.admin_config
        )
        self.databases = database_stack.databases

    def _deploy_roles(self):
        """Deploy role resources"""
        role_stack = RoleStack(
            self,
            "Roles",
            snow=self.snow,
            admin_config=self.admin_config,
            databases=self.databases
        )
        self.roles = role_stack.roles

    def _deploy_warehouses(self):
        """Deploy warehouse resources"""
        warehouse_stack = WarehouseStack(
            self,
            "Warehouses",
            snow=self.snow,
            admin_config=self.admin_config
        )
        self.warehouses = warehouse_stack.warehouses
