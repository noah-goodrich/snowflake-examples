"""Base table adapter for Snowflake tables."""

from typing import Dict, Any, Optional, List
from snowflake.core import Root
from snowflake.core.table import Table as SnowflakeTable

from specs.table import BaseTableSpec, StandardTableSpec, HybridTableSpec, DynamicTableSpec


class TableAdapter:
    """Base class for table adapters."""

    def __init__(self, root: Root):
        """Initialize the adapter with a Snowflake root object."""
        self.root = root

    def create(self, spec: BaseTableSpec) -> SnowflakeTable:
        """Create a table from a specification."""
        raise NotImplementedError

    def alter(self, spec: BaseTableSpec) -> SnowflakeTable:
        """Alter a table from a specification."""
        raise NotImplementedError

    def drop(self, spec: BaseTableSpec) -> None:
        """Drop a table from a specification."""
        raise NotImplementedError

    def get(self, spec: BaseTableSpec) -> Optional[SnowflakeTable]:
        """Get a table from a specification."""
        raise NotImplementedError
