from constructs import Construct
from typing import Any, Dict
from snowflake.core import Root
from snowflake.core.warehouse import Warehouse
from snowflake.core.database import Database
from snowflake.core.role import Role
from snowflake.core.user import User
from .snow import SnowStack

ADMIN_ROLE_NAME = 'HOID'
ADMIN_DATABASE_NAME = 'COSMERE'


class Admin(SnowStack):
    def deploy(self):
        try:
            self.deploy_admin_role_and_user()
            
            # Create admin warehouse
            warehouse = self.create_or_alter_warehouse(
                name=self.admin_config['admin_warehouse']['name'],
                warehouse_size=self.admin_config['admin_warehouse']['size'].upper(
                ),
                auto_suspend=self.admin_config['admin_warehouse']['auto_suspend'],
                auto_resume=self.admin_config['admin_warehouse']['auto_resume'],
                initially_suspended=self.admin_config['admin_warehouse']['initially_suspended']
            )

            # Create COSMERE database
            database = self.create_or_alter_database(
                name='COSMERE',
                comment=self.admin_config['databases']['COSMERE']['comment']
            )

            # Create schemas
            for schema_name in self.admin_config['databases']['COSMERE']['schemas']:
                database.create_schema_if_not_exists(schema_name)

                # Store service account info
            self.svc_admin = {
                'name': 'SVC_HOID',
                'password': self.svc_admin_secret['password']
            }

        except Exception as e:
            raise Exception(f"Failed to create admin resources: {e}")

    def deploy_admin_role_and_user(self):
        self.create_or_alter_functional_role(
            name='HOID',
            comment='Administrative role for COSMERE'
        )

        self.snow.security.grants.grant_role_to_role(
            'SECURITYADMIN', ADMIN_ROLE_NAME)
        self.snow.security.grants.grant_role_to_role(
            'SYSADMIN', ADMIN_ROLE_NAME)

        self.create_user_if_not_exists(
            name='SVC_HOID',
            default_role=ADMIN_ROLE_NAME,
            comment='Service account for administrative automation'
        )

        self.snow.security.grants.grant_role_to_user(
            ADMIN_ROLE_NAME, 'SVC_HOID')
