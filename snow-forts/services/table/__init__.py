"""Table service for Snowflake tables."""

from typing import Optional, List, Dict, Any
from snowflake.core import Root
from snowflake.core.table import Table as SnowflakeTable
from snowflake.core.table import TableColumn

from .adapters.base import TableAdapter
from .adapters.standard import StandardTableAdapter
from .adapters.hybrid import HybridTableAdapter
from .adapters.dynamic import DynamicTableAdapter
from specs.table import BaseTableSpec, StandardTableSpec, HybridTableSpec, DynamicTableSpec


class TableService:
    """Service for managing Snowflake tables.

    This service provides high-level operations for managing tables in Snowflake,
    delegating the actual implementation details to appropriate adapters.
    """

    def __init__(self, root: Root):
        """Initialize the table service.

        Args:
            root: Snowflake root object for database operations
        """
        self.root = root

    def create(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Create a table from a specification."""
        try:
            adapter = self._get_adapter(spec)
            return adapter.create(spec)
        except Exception as e:
            print(f"Error creating table: {e}")
            return False

    def alter(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Alter a table from a specification."""
        try:
            adapter = self._get_adapter(spec)
            return adapter.alter(spec)
        except Exception as e:
            print(f"Error altering table: {e}")
            return False

    def drop(self, spec: BaseTableSpec) -> Dict[str, Any]:
        """Drop a table from a specification."""
        try:
            adapter = self._get_adapter(spec)
            return adapter.drop(spec)
        except Exception as e:
            print(f"Error dropping table: {e}")
            return False

    def get(self, spec: BaseTableSpec) -> Optional[Dict[str, Any]]:
        """Get a table from a specification."""
        try:
            adapter = self._get_adapter(spec)
            return adapter.get(spec)
        except Exception as e:
            print(f"Error getting table: {e}")
            return None

    def exists(self, spec: BaseTableSpec) -> bool:
        """Check if a table exists.

        Args:
            spec: Table specification defining the table to check

        Returns:
            bool: True if table exists, False otherwise
        """
        try:
            adapter = self._get_adapter(spec)
            return adapter.exists(spec)
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    def get_columns(self, spec: BaseTableSpec) -> List[TableColumn]:
        """Get the columns of an existing table.

        Args:
            spec: Table specification defining the table to get columns from

        Returns:
            List[TableColumn]: List of table columns
        """
        try:
            adapter = self._get_adapter(spec)
            table = adapter.get(spec)
            if table:
                return table.columns
            return []
        except Exception as e:
            print(f"Error getting table columns: {e}")
            return []

    def _get_adapter(self, spec: BaseTableSpec) -> TableAdapter:
        """Get the appropriate adapter for a specification."""
        if isinstance(spec, StandardTableSpec):
            return StandardTableAdapter(self.root)
        elif isinstance(spec, HybridTableSpec):
            return HybridTableAdapter(self.root)
        elif isinstance(spec, DynamicTableSpec):
            return DynamicTableAdapter(self.root)
        else:
            raise ValueError(f"Unknown table spec type: {type(spec)}")
