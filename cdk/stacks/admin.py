from constructs import Construct
from typing import Any, Dict
from snowflake.core import Root
from snowflake.core.warehouse import Warehouse
from snowflake.core.database import Database
from snowflake.core.role import Role
from snowflake.core.user import User
from stacks.snow_stack import SnowStack


class AdminStack(SnowStack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        snow: Root,
        admin_role: Dict[str, Any],
        svc_admin_secret: Dict[str, Any],
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        self.admin_config = self.load_config(
            'stacks/snowflake/config/admin.yaml')

    def deploy(self):
        try:
            # Create admin warehouse
            warehouse = self.create_warehouse(
                self.snow,
                name=self.admin_config['admin_warehouse']['name'],
                warehouse_size=self.admin_config['admin_warehouse']['size'].upper(
                ),
                auto_suspend=self.admin_config['admin_warehouse']['auto_suspend'],
                auto_resume=self.admin_config['admin_warehouse']['auto_resume'],
                initially_suspended=self.admin_config['admin_warehouse']['initially_suspended']
            )

            # Create COSMERE database
            database = Database.create_if_not_exists(
                self.snow,
                name='COSMERE',
                comment=self.admin_config['databases']['COSMERE']['comment']
            )

            # Create schemas
            for schema_name in self.admin_config['databases']['COSMERE']['schemas']:
                database.create_schema_if_not_exists(schema_name)

            # Create SVC_ADMIN role
            admin = Role.create_if_not_exists(
                self.snow,
                name=self.admin_role['name'],
                comment=self.admin_role['comment']
            )

            # Grant system roles to SVC_ADMIN
            self.snow.security.grants.grant_role_to_role(
                'SECURITYADMIN', self.admin_role['name'])
            self.snow.security.grants.grant_role_to_role(
                'SYSADMIN', self.admin_role['name'])

            # Grant warehouse access
            warehouse.grants.grant_privilege_to_role(
                self.admin_role['name'],
                ['USAGE', 'OPERATE', 'MONITOR']
            )

            # Grant database privileges
            database.grants.grant_ownership_to_role(self.admin_role['name'])
            database.grants.revoke_ownership_from_role('ACCOUNTADMIN')

            # Create SVC_ADMIN user
            svc_admin = User.create_if_not_exists(
                self.snow,
                name='SVC_HOID',
                password=self.svc_admin_secret['password'],
                default_role=self.admin_role['name'],
                default_warehouse=self.admin_config['admin_warehouse']['name'],
                comment='Service account for administrative automation'
            )

            # Grant SVC_ADMIN role to service account
            self.snow.security.grants.grant_role_to_user(
                self.admin_role['name'], 'SVC_HOID')

            # Store service account info
            self.svc_admin = {
                'name': 'SVC_HOID',
                'password': self.svc_admin_secret['password']
            }

        except Exception as e:
            raise Exception(f"Failed to create admin resources: {e}")
