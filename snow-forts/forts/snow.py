"""Base class for Snowflake infrastructure management"""

from typing import Any, Dict, Optional
from snowflake.core import Root
from state_managers.warehouse import WarehouseStateManager
from state_managers.database import DatabaseStateManager
from state_managers.role import RoleStateManager
from state_managers.user import UserStateManager
from aws.secrets import SecretsManager


class SnowFort:
    """Base class for Snowflake infrastructure management"""

    def __init__(
        self,
        snow: Root,
        environment: str,
        warehouse_state: Optional[WarehouseStateManager] = None,
        database_state: Optional[DatabaseStateManager] = None,
        role_state: Optional[RoleStateManager] = None,
        user_state: Optional[UserStateManager] = None,
        secrets_manager: Optional[SecretsManager] = None
    ):
        """Initialize a new SnowFort instance.

        Args:
            snow: Authenticated Snowflake connection
            environment: Environment name (e.g. dev, prod)
            warehouse_state: Optional WarehouseStateManager instance
            database_state: Optional DatabaseStateManager instance
            role_state: Optional RoleStateManager instance
            user_state: Optional UserStateManager instance
            secrets_manager: Optional SecretsManager instance for AWS operations
        """
        self.snow = snow
        self.environment = environment
        self.warehouse_state = warehouse_state or WarehouseStateManager(snow)
        self.database_state = database_state or DatabaseStateManager(snow)
        self.role_state = role_state or RoleStateManager(snow)
        self.user_state = user_state or UserStateManager(snow)
        self.secrets_manager = secrets_manager or SecretsManager()

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager"""
        return self.secrets_manager.get_secret(secret_name)

    def deploy(self):
        """Deploy infrastructure. To be implemented by subclasses."""
        pass
