from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.role import Role as SnowflakeRole, Securable as RoleSecurable


@dataclass
class RoleConfig:
    """Configuration for role operations"""
    name: str
    comment: Optional[str] = None
    granted_roles: Optional[List[str]] = None
    prefix_with_environment: bool = True

    def validate(self) -> None:
        """Validates role configuration"""
        if not self.name:
            raise ValueError("Role name cannot be empty")
        if self.granted_roles and not all(self.granted_roles):
            raise ValueError("Granted role names cannot be empty")


class Role:
    """Manages Snowflake role operations"""

    def __init__(self, snow: Root, environment: str):
        self.snow = snow
        self.environment = environment

    def create(self, config: RoleConfig, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeRole:
        """Creates a new role"""
        config.validate()
        name = self._format_name(config.name, config.prefix_with_environment)

        # Create role
        role = self.snow.roles.create(
            SnowflakeRole(
                name=name,
                comment=config.comment
            ),
            mode=mode
        )

        # Grant other roles if specified
        if config.granted_roles:
            for granted_role in config.granted_roles:
                role.grant_role(
                    role_type='ROLE',
                    role=RoleSecurable(name=granted_role)
                )

        return role

    def alter(self, name: str, config: RoleConfig) -> SnowflakeRole:
        """Alters an existing role"""
        config.validate()
        role = self.get(name)
        if not role:
            raise ValueError(f"Role {name} does not exist")

        # Update role properties
        return self.create(config, mode=CreateMode.or_replace)

    def drop(self, name: str, cascade: bool = True) -> None:
        """Drops a role"""
        role = self.get(name)
        if role:
            role.drop(cascade=cascade)

    def get(self, name: str) -> Optional[SnowflakeRole]:
        """Gets a role by name"""
        try:
            return self.snow.roles[self._format_name(name, True)]
        except KeyError:
            try:
                return self.snow.roles[name.upper()]
            except KeyError:
                return None

    def grant_privilege(self, role_name: str, privilege: str, on_type: str, on_name: str) -> None:
        """Grants a privilege to a role"""
        role = self.get(role_name)
        if not role:
            raise ValueError(f"Role {role_name} does not exist")

        self.snow.session.sql(
            f"GRANT {privilege} ON {on_type} {on_name} TO ROLE {role.name}"
        ).collect()

    def revoke_privilege(self, role_name: str, privilege: str, on_type: str, on_name: str) -> None:
        """Revokes a privilege from a role"""
        role = self.get(role_name)
        if not role:
            raise ValueError(f"Role {role_name} does not exist")

        self.snow.session.sql(
            f"REVOKE {privilege} ON {on_type} {on_name} FROM ROLE {role.name}"
        ).collect()

    def grant_role(self, role_name: str, granted_role: str) -> None:
        """Grants one role to another"""
        role = self.get(role_name)
        if not role:
            raise ValueError(f"Role {role_name} does not exist")

        role.grant_role(
            role_type='ROLE',
            role=RoleSecurable(name=granted_role)
        )

    def revoke_role(self, role_name: str, revoked_role: str) -> None:
        """Revokes one role from another"""
        role = self.get(role_name)
        if not role:
            raise ValueError(f"Role {role_name} does not exist")

        self.snow.session.sql(
            f"REVOKE ROLE {revoked_role} FROM ROLE {role.name}"
        ).collect()

    def _format_name(self, name: str, use_environment: bool) -> str:
        """Formats role name according to conventions"""
        if use_environment:
            return f"{self.environment}_{name}".upper()
        return name.upper()
