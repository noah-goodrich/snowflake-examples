from typing import Any, Dict, Optional, List
import boto3
from botocore.exceptions import ClientError
from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.database import Database
from snowflake.core.database_role import DatabaseRole, ContainingScope as DatabaseContainingScope, Securable as DatabaseSecurable
from snowflake.core.role import Role, ContainingScope as RoleContainingScope, Securable as RoleSecurable
from snowflake.core.warehouse import Warehouse
import os


class SnowFort():

    def __init__(self, snow: Root, environment: str = None, botocore_session: boto3.Session = None) -> None:
        # Get environment from env var if not provided
        self.env = (type(environment) == str and environment.lower()) \
            or os.getenv('SNOWFLAKE_ENV', 'dev').lower()
        if self.env not in ['dev', 'stg', 'prd']:
            raise ValueError(f"Invalid environment: {self.environment}")

        # Initialize properties
        self.snow = snow
        self.boto = botocore_session or boto3.session.Session()

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager"""
        client = self.boto.client(service_name='secretsmanager')
        try:
            return client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise Exception(f"Failed to get secret {secret_name}: {e}")

    def create_if_not_exists_database(self, name: str, description: str, prefix_with_environment: bool = True) -> 'SnowStack':
        """Create or alter a database with the given configuration.
        Args:
            db: Database name
            description: Database description
            prefix_with_environment: Whether to prefix the database name with the environment
        """
        if prefix_with_environment:
            db = f"{self.env}_{name}"
        else:
            db = name

        self.snow.databases[db].create_or_alter(Database(
            name=db,
            comment=description
        ))
        self.snow.session.use_database(db)

        roles = self.snow.databases[db].database_roles
        ro = roles.create(DatabaseRole(
            name=f"READ_ONLY",
            comment="Read-only role for database"
        ), mode=CreateMode.if_not_exists)
        ro.grant_privileges_on_all(['SELECT'], 'TABLE', containing_scope=DatabaseContainingScope(
            database=db
        ))
        ro.grant_future_privileges(['SELECT'], 'TABLE', containing_scope=DatabaseContainingScope(
            database=db
        ))

        rw = roles.create(DatabaseRole(
            name=f"READ_WRITE",
            comment="Read-write role for database"
        ), mode=CreateMode.if_not_exists)

        rw.grant_role(role_type='DATABASE ROLE',
                      role=DatabaseSecurable(name=ro.name))
        rw.grant_privileges_on_all(
            ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'TABLE',
            containing_scope=DatabaseContainingScope(database=db)
        )
        rw.grant_future_privileges(
            ['SELECT', 'INSERT', 'UPDATE', 'DELETE'],
            'TABLE',
            containing_scope=DatabaseContainingScope(database=db)
        )

        owner = self.snow.roles.create(Role(
            name=f"{db}_OWNER",
            comment="Owner role for database"
        ), mode=CreateMode.if_not_exists)

        owner.grant_privileges(
            ['OWNERSHIP'], 'DATABASE', securable=RoleSecurable(name=db))
        owner.grant_future_privileges(
            ['OWNERSHIP'], 'SCHEMA', containing_scope=RoleContainingScope(database=db))
        owner.grant_future_privileges(
            ['OWNERSHIP'], 'TABLE', containing_scope=RoleContainingScope(database=db))
        owner.grant_role(role_type='DATABASE ROLE',
                         role=RoleSecurable(database=db, name=rw.name))

        current_role = self.snow.roles[self.snow.session.get_current_role()]
        current_role.grant_role(role_type='ROLE',
                                role=RoleSecurable(name=owner.name))

        self.snow

        return self

    def create_or_alter_warehouse(self, db: str, size: str, overrides: dict = {}) -> 'SnowFort':
        """Create or alter a warehouse with the given configuration.
        Args:
            db: Database name
            size: Warehouse size
        """
        defaults = {
            'warehouse_size': size,
            'warehouse_type': 'STANDARD',
            'auto_suspend': 5,
            'auto_resume': 'true',
            'initially_suspended': 'true',
            'min_cluster_count': 1,
            'max_cluster_count': 1,
            'scaling_policy': 'STANDARD',
            'prefix_with_environment': True
        }

        config = {**defaults, **overrides}

        prefix = config.get('prefix_with_environment')

        del config['prefix_with_environment']

        if prefix:
            config['name'] = f"{self.env}_{db.lower()}_{size.lower()}"
        else:
            config['name'] = f"{db.lower()}_{size.lower()}"

        # Create or alter warehouse
        self.snow.warehouses[config['name']
                             ].create_or_alter(Warehouse(**config))

        return self

    def create_or_alter_functional_role(
            self,
            name: str,
            description: str,
            access_roles: List[str],
            grants_to: Optional[List[str]] = None
    ) -> 'SnowFort':
        """Create or alter a functional role that inherits from access roles.

        Functional roles should never be granted direct access to resources.
        Instead, they should inherit from appropriate access roles.

        Example usage:
            # Create ML Engineer role with specific database access
            stack.create_or_alter_functional_role(
                name="ML_ENGINEER",
                description="Machine Learning Engineer access pattern",
                access_roles=[
                    "DEV_BRONZE_RO",
                    "DEV_SILVER_RW",
                    "DEV_GOLD_RO",
                    "DEV_PLATINUM_RW"
                ]
            )

            # Create Data Engineer role
            stack.create_or_alter_functional_role(
                name="DATA_ENGINEER",
                description="Data Engineer access pattern",
                access_roles=[
                    "DEV_BRONZE_RW",
                    "DEV_SILVER_RW",
                    "DEV_GOLD_RW"
                ],
                grants_to=["ML_ENGINEER"]  # Optional inheritance
            )
        """
        # Create or replace role
        self.snow.roles[name].create_or_alter(Role(
            name=name,
            comment=description
        ))

        # Grant access roles
        for role in access_roles:
            self.snow.roles[name].grant_role(role)

        # Grant role inheritance
        if grants_to:
            for grant_role in grants_to:
                self.snow.roles[name].grant_role(grant_role)

        return self

    def create_or_alter_user(self, user: str, config: dict) -> 'SnowStack':
        """Create a user with the given configuration"""
        pass

    def deploy(self):
        """Deploy all resources defined in the stack"""
        raise NotImplementedError(
            "Deploy method needs to be defined in an implementing class")
