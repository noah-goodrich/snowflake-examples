"""Standard table adapter for Snowflake tables."""

from typing import Optional, List, Dict, Any

from snowflake.core import Root, CreateMode
from snowflake.core.table import Table as SnowflakeTable

from specs.table import StandardTableSpec, ColumnOperation
from .base import TableAdapter


class StandardTableAdapter(TableAdapter):
    """Adapter for standard tables."""

    def __init__(self, snow: Root, spec: StandardTableSpec, formatted_name: str, formatted_schema: str):
        """Initialize the adapter.

        Args:
            snow: Snowflake root object
            spec: Table specification
            formatted_name: Formatted table name (with environment prefix)
            formatted_schema: Formatted schema name (with environment prefix)
        """
        super().__init__(snow, spec, formatted_name, formatted_schema)

    def create(self, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeTable:
        """Create a standard table.

        Args:
            mode: Creation mode (if_not_exists, or_replace, error_if_exists)

        Returns:
            Created Snowflake table
        """
        table = self.get_snowflake_table()
        if table and mode == CreateMode.error_if_exists:
            raise ValueError(
                f"Table {self.get_qualified_name()} already exists")
        elif table and mode == CreateMode.or_replace:
            self.drop()
        elif table and mode == CreateMode.if_not_exists:
            return table

        # Create new table
        table = SnowflakeTable(
            name=self.formatted_name,
            schema=self.formatted_schema,
            columns=self.spec.columns,
            comment=self.spec.comment
        )
        table.create()
        return table

    def alter(self, column_operations: Optional[List[ColumnOperation]] = None) -> None:
        """Alter a standard table.

        Args:
            column_operations: Column operations to perform
        """
        table = self.get_snowflake_table()
        if not table:
            raise ValueError(
                f"Table {self.get_qualified_name()} does not exist")

        if column_operations:
            for op in column_operations:
                if op.operation == "add":
                    table.add_column(op.column)
                elif op.operation == "drop":
                    table.drop_column(op.column.name)
                elif op.operation == "modify":
                    table.alter_column(op.column)
                else:
                    raise ValueError(
                        f"Unknown column operation: {op.operation}")

        if self.spec.comment != table.comment:
            table.set_comment(self.spec.comment)

    def drop(self) -> None:
        """Drop a standard table."""
        table = self.get_snowflake_table()
        if table:
            table.drop()

    def get(self) -> Optional[Dict[str, Any]]:
        """Get standard table details.

        Returns:
            Table details if found, None otherwise
        """
        table = self.get_snowflake_table()
        if not table:
            return None

        return {
            "name": table.name,
            "schema": table.schema_name,
            "columns": table.columns,
            "comment": table.comment
        }

    def exists(self) -> bool:
        """Check if standard table exists."""
        return bool(self.get())
