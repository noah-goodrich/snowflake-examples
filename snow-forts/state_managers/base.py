"""Base state manager interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, TypeVar

T = TypeVar('T')


class StateManager(ABC, Generic[T]):
    """Base interface for state managers."""

    @abstractmethod
    def apply(self, spec: T) -> Dict[str, Any]:
        """Apply the desired state for a resource.

        Args:
            spec: The specification to apply

        Returns:
            Dictionary containing operation results
        """
        pass
