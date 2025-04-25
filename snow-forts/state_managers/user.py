"""User state manager implementation."""

from typing import Dict, Any, Optional, List
from snowflake.core import Root

from specs.user import UserSpec, UserType
from state_managers.types import StateChangeMetadata


class UserOperation:
    """Base class for user operations."""

    def __init__(self, root: Root):
        """Initialize the operation with a Snowflake root object."""
        self.root = root

    def execute(self, spec: UserSpec) -> Dict[str, Any]:
        """Execute the operation."""
        raise NotImplementedError


class CreateUserOperation(UserOperation):
    """Operation for creating a user."""

    def execute(self, spec: UserSpec) -> Dict[str, Any]:
        """Create a user from a specification."""
        from services.user import UserService
        service = UserService(self.root)
        return service.create(spec)


class AlterUserOperation(UserOperation):
    """Operation for altering a user."""

    def execute(self, spec: UserSpec) -> Dict[str, Any]:
        """Alter a user from a specification."""
        from services.user import UserService
        service = UserService(self.root)
        return service.alter(spec)


class DropUserOperation(UserOperation):
    """Operation for dropping a user."""

    def execute(self, spec: UserSpec) -> Dict[str, Any]:
        """Drop a user from a specification."""
        from services.user import UserService
        service = UserService(self.root)
        return service.drop(spec)


class UserStateManager:
    """Manager for user state operations."""

    def __init__(self, root: Root):
        """Initialize the manager with a Snowflake root object."""
        self.root = root

    def apply(self, current: Optional[UserSpec], desired: UserSpec) -> StateChangeMetadata:
        """Apply state changes to match desired state."""
        if current is None:
            # Create new user
            operation = CreateUserOperation(self.root)
            result = operation.execute(desired)
            return StateChangeMetadata(
                operation="CREATE",
                successful=True,
                created=True,
                dropped=False,
                metadata=result
            )
        elif current != desired:
            # Alter existing user
            operation = AlterUserOperation(self.root)
            result = operation.execute(desired)
            return StateChangeMetadata(
                operation="ALTER",
                successful=True,
                created=False,
                dropped=False,
                metadata=result
            )
        else:
            # No changes needed
            return StateChangeMetadata(
                operation="NOOP",
                successful=True,
                created=False,
                dropped=False,
                metadata={}
            )

    def destroy(self, spec: UserSpec) -> StateChangeMetadata:
        """Destroy a user."""
        operation = DropUserOperation(self.root)
        result = operation.execute(spec)
        return StateChangeMetadata(
            operation="DROP",
            successful=True,
            created=False,
            dropped=True,
            metadata=result
        )
