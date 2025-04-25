"""Common types for state management."""

from typing import Dict, Any
from dataclasses import dataclass


@dataclass
class OperationResult:
    """Result of a state operation.

    Attributes:
        operation: Name of the operation performed
        successful: Whether the operation was successful
        created: Whether a resource was created
        dropped: Whether a resource was dropped
        metadata: Additional operation metadata
    """
    operation: str
    successful: bool
    created: bool = False
    dropped: bool = False
    metadata: Dict[str, Any] = None


@dataclass
class StateChangeMetadata:
    """Metadata about a state change operation."""
    operation: str  # CREATE, ALTER, DROP, NOOP
    successful: bool
    created: bool
    dropped: bool
    metadata: Dict[str, Any]
