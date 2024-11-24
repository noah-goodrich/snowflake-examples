from aws_cdk import Stack
from constructs import Construct
from typing import Any, Dict

from .databases import DatabaseStack
from .roles import RoleStack
from .warehouses import WarehouseStack


class SnowflakeStack(Stack):
    def __init__(self, scope: Construct, id: str, snowflake_secret: Dict[str, Any], **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create role stack
        roles = RoleStack(self, "Roles",
                          snowflake_secret=snowflake_secret)

        # Create database stack
        databases = DatabaseStack(self, "Databases",
                                  snowflake_secret=snowflake_secret)

        # Create warehouse stack
        warehouses = WarehouseStack(self, "Warehouses",
                                    snowflake_secret=snowflake_secret)

        # Add dependencies
        databases.add_dependency(roles)
        warehouses.add_dependency(roles)
