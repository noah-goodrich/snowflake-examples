"""Dynamic table adapter for Snowflake tables."""

from typing import Optional, List, Dict, Any

from snowflake.core import Root, CreateMode
from snowflake.core.table import Table as SnowflakeTable

from specs.table import DynamicTableSpec, ColumnOperation
from .base import TableAdapter


class DynamicTableAdapter(TableAdapter):
    """Adapter for dynamic tables."""

    def __init__(self, snow: Root, spec: DynamicTableSpec, formatted_name: str, formatted_schema: str):
        """Initialize the adapter.

        Args:
            snow: Snowflake root object
            spec: Table specification
            formatted_name: Formatted table name (with environment prefix)
            formatted_schema: Formatted schema name (with environment prefix)
        """
        super().__init__(snow, spec, formatted_name, formatted_schema)

    def create(self, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeTable:
        """Create a dynamic table.

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
            target_lag=self.spec.target_lag,
            warehouse=self.spec.warehouse,
            schedule=self.spec.schedule,
            query=self.spec.query
        )
        table.create()
        return table

    def alter(self, column_operations: Optional[List[ColumnOperation]] = None) -> None:
        """Alter a dynamic table.

        Args:
            column_operations: Column operations to perform
        """
        # For dynamic tables, we need to alter the query
        qualified_name = self.get_qualified_name()

        # Build alterations
        alterations = []

        if self.spec.query:
            alterations.append(f"SET QUERY = {self.spec.query}")

        if self.spec.comment:
            alterations.append(f"SET COMMENT = '{self.spec.comment}'")

        if self.spec.dynamic_settings.target_lag:
            alterations.append(
                f"SET TARGET_LAG = '{self.spec.dynamic_settings.target_lag}'")

        if self.spec.dynamic_settings.warehouse:
            alterations.append(
                f"SET WAREHOUSE = {self.spec.dynamic_settings.warehouse}")

        if self.spec.dynamic_settings.refresh_mode:
            alterations.append(
                f"SET REFRESH_MODE = {self.spec.dynamic_settings.refresh_mode}")

        if self.spec.dynamic_settings.refresh_schedule:
            alterations.append(
                f"SET SCHEDULE = '{self.spec.dynamic_settings.refresh_schedule}'")

        # Execute alterations if any
        if alterations:
            alter_sql = f"ALTER DYNAMIC TABLE {qualified_name} {' '.join(alterations)}"
            self.snow.sql(alter_sql).collect()

    def drop(self) -> None:
        """Drop a dynamic table."""
        qualified_name = self.get_qualified_name()
        self.snow.sql(
            f"DROP DYNAMIC TABLE IF EXISTS {qualified_name}").collect()

    def get(self) -> Optional[Dict[str, Any]]:
        """Get dynamic table details.

        Returns:
            Table details if found, None otherwise
        """
        try:
            # Execute SQL to get table details
            qualified_name = self.get_qualified_name()
            sql = f"""
            SELECT 
                name,
                schema_name,
                comment,
                cluster_by,
                query,
                'DYNAMIC' as kind,
                target_lag,
                warehouse
            FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES())
            WHERE schema_name = '{self.formatted_schema}'
            AND name = '{self.formatted_name}'
            """

            result = self.snow.sql(sql).collect()

            if not result:
                return None

            return result[0]
        except Exception:
            return None
