"""Hybrid table adapter implementation."""

from typing import Optional, List, Dict, Any

from snowflake.core import Root, CreateMode
from snowflake.core.table import Table as SnowflakeTable

from specs.table import HybridTableSpec, ColumnOperation
from .base import TableAdapter


class HybridTableAdapter(TableAdapter):
    """Adapter for hybrid tables."""

    def __init__(self, snow: Root, spec: HybridTableSpec, formatted_name: str, formatted_schema: str):
        """Initialize the adapter.

        Args:
            snow: Snowflake root object
            spec: Table specification
            formatted_name: Formatted table name (with environment prefix)
            formatted_schema: Formatted schema name (with environment prefix)
        """
        super().__init__(snow, spec, formatted_name, formatted_schema)

    def create(self, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeTable:
        """Create a hybrid table.

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
            comment=self.spec.comment,
            clustering_keys=self.spec.clustering_keys
        )
        table.create()
        return table

    def alter(self, column_operations: Optional[List[ColumnOperation]] = None) -> None:
        """Alter a hybrid table.

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

        if self.spec.clustering_keys != table.clustering_keys:
            table.set_clustering_keys(self.spec.clustering_keys)

    def drop(self) -> None:
        """Drop a hybrid table."""
        table = self.get_snowflake_table()
        if table:
            table.drop()

    def get(self) -> Optional[Dict[str, Any]]:
        """Get hybrid table details.

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
            "comment": table.comment,
            "clustering_keys": table.clustering_keys
        }
