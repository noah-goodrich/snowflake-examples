"""Base service interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict


class Service(ABC):
    """Base interface for services."""

    @abstractmethod
    def create(self, resource: Any) -> Dict[str, Any]:
        """Create a resource.

        Args:
            resource: The resource to create

        Returns:
            Dictionary containing operation results
        """
        pass

    @abstractmethod
    def alter(self, resource: Any) -> Dict[str, Any]:
        """Alter a resource.

        Args:
            resource: The resource to alter

        Returns:
            Dictionary containing operation results
        """
        pass

    @abstractmethod
    def drop(self, resource: Any) -> Dict[str, Any]:
        """Drop a resource.

        Args:
            resource: The resource to drop

        Returns:
            Dictionary containing operation results
        """
        pass
