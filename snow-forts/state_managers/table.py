"""Table state manager for Snowflake tables."""

from typing import Dict, Any, Optional, List
from snowflake.core import Root

from specs.table import BaseTableSpec, StandardTableSpec, HybridTableSpec, DynamicTableSpec
from state_managers.types import StateChangeMetadata


class TableOperation:
    """Base class for table operations."""

    def __init__(self, root: Root):
        """Initialize the operation with a Snowflake root object."""
        self.root = root

    def execute(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Execute the operation."""
        raise NotImplementedError


class CreateTableOperation(TableOperation):
    """Operation for creating a table."""

    def execute(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Create a table from a specification."""
        from services.table import TableService
        service = TableService(self.root)
        return service.create(spec)


class AlterTableOperation(TableOperation):
    """Operation for altering a table."""

    def execute(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Alter a table from a specification."""
        from services.table import TableService
        service = TableService(self.root)
        return service.alter(spec)


class DropTableOperation(TableOperation):
    """Operation for dropping a table."""

    def execute(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Drop a table from a specification."""
        from services.table import TableService
        service = TableService(self.root)
        return service.drop(spec)


class TableStateManager:
    """Manager for table state operations."""

    def __init__(self, root: Root):
        """Initialize the manager with a Snowflake root object."""
        self.root = root

    def apply(self, current: Optional[BaseTableSpec], desired: BaseTableSpec) -> StateChangeMetadata:
        """Apply state changes to match desired state."""
        if current is None:
            # Create new table
            operation = CreateTableOperation(self.root)
            result = operation.execute(desired)
            return StateChangeMetadata(
                operation="CREATE",
                successful=True,
                created=True,
                dropped=False,
                metadata=result
            )
        elif current != desired:
            # Alter existing table
            operation = AlterTableOperation(self.root)
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

    def destroy(self, spec: BaseTableSpec) -> StateChangeMetadata:
        """Destroy a table."""
        operation = DropTableOperation(self.root)
        result = operation.execute(spec)
        return StateChangeMetadata(
            operation="DROP",
            successful=True,
            created=False,
            dropped=True,
            metadata=result
        )
