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
    """A class for managing Snowflake resources and configurations.

    This class provides methods for creating and managing Snowflake databases,
    warehouses, roles, and users with AWS integration support.

    Attributes:
        env (str): The environment (dev/stg/prd)
        snow (Root): Snowflake Root instance
        boto (Session): AWS boto3 session
    """

    def __init__(self, snow: Root, environment: str = None, botocore_session: boto3.Session = None) -> None:
        """Initialize a new SnowFort instance.

        Args:
            snow (Root): Snowflake Root instance
            environment (str, optional): Environment name (dev/stg/prd). Defaults to None.
            botocore_session (boto3.Session, optional): AWS boto3 session. Defaults to None.

        Raises:
            ValueError: If environment is invalid
        """
        # Get environment from env var if not provided
        self.env = (type(environment) == str and environment.lower()) \
            or os.getenv('SNOWFLAKE_ENV', 'dev').lower()
        if self.env not in ['dev', 'stg', 'prd']:
            raise ValueError(f"Invalid environment: {self.environment}")

        # Initialize properties
        self.snow = snow
        self.boto = botocore_session or boto3.session.Session()

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Retrieve a secret from AWS Secrets Manager.

        Args:
            secret_name (str): Name of the secret to retrieve

        Returns:
            Dict[str, Any]: The secret value

        Raises:
            Exception: If secret retrieval fails
        """
        client = self.boto.client(service_name='secretsmanager')
        try:
            return client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise Exception(f"Failed to get secret {secret_name}: {e}")

    def create_if_not_exists_database(self, name: str, description: str, prefix_with_environment: bool = True) -> 'SnowStack':
        """Create or alter a database with associated roles and permissions.

        Creates a database with READ_ONLY and READ_WRITE database roles, and an OWNER role.
        All roles are configured with appropriate privileges and inheritance.

        Args:
            name (str): Database name
            description (str): Database description
            prefix_with_environment (bool, optional): Whether to prefix database name with environment. Defaults to True.

        Returns:
            SnowStack: The current instance for method chaining
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
        """Create or alter a Snowflake warehouse.

        Args:
            db (str): Database name (used in warehouse naming)
            size (str): Warehouse size (e.g., 'XSMALL', 'SMALL', etc.)
            overrides (dict, optional): Override default warehouse settings. Defaults to {}.

        Returns:
            SnowFort: The current instance for method chaining

        Note:
            Default settings include:
            - warehouse_type: STANDARD
            - auto_suspend: 5
            - auto_resume: true
            - initially_suspended: true
            - min_cluster_count: 1
            - max_cluster_count: 1
            - scaling_policy: STANDARD
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
        """Create or alter a functional role with inherited permissions.

        Creates a role that inherits from specified access roles rather than having
        direct resource access. Optionally allows other roles to inherit from it.

        Args:
            name (str): Role name
            description (str): Role description
            access_roles (List[str]): List of access roles to inherit from
            grants_to (Optional[List[str]], optional): List of roles that can inherit this role. Defaults to None.

        Returns:
            SnowFort: The current instance for method chaining
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

    def deploy(self):
        """Deploy all resources defined in the stack.

        Raises:
            NotImplementedError: This method must be implemented by subclasses
        """
        raise NotImplementedError(
            "Deploy method needs to be defined in an implementing class")
