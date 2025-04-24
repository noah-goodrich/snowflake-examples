"""Table specification models for configuration."""

from typing import List, Optional, Literal, Union, Dict, Any, Annotated
from pydantic import BaseModel, Field, model_validator, field_validator, ConfigDict


class ColumnSpec(BaseModel):
    """Column specification."""

    name: str = Field(..., description="Column name")
    datatype: str = Field(..., description="Column data type")
    nullable: bool = Field(True, description="Whether the column is nullable")
    default: Optional[str] = Field(
        None, description="Default value for the column")
    comment: Optional[str] = Field(None, description="Column comment")
    primary_key: bool = Field(
        False, description="Whether the column is part of the primary key")

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        """Validate column name."""
        if not v:
            raise ValueError("Column name cannot be empty")
        return v

    @field_validator('datatype')
    @classmethod
    def validate_datatype(cls, v, info):
        """Validate data type."""
        values = info.data
        if 'operation' in values and values['operation'] == 'DROP':
            # For DROP operations, we don't need a datatype
            return v
        if not v:
            raise ValueError("Data type cannot be empty")
        return v


class ColumnOperation(BaseModel):
    """Column operation for ALTER TABLE."""

    operation: Literal["ADD", "DROP", "ALTER",
                       "RENAME"] = Field(..., description="Operation type")
    column: ColumnSpec = Field(..., description="Column specification")
    new_name: Optional[str] = Field(
        None, description="New name for RENAME operation")

    @model_validator(mode='after')
    def validate_operation(self):
        """Validate operation specific fields."""
        if self.operation == "RENAME" and not self.new_name:
            raise ValueError("RENAME operation requires new_name")
        return self


class BaseTableSpec(BaseModel):
    """Base table specification model."""

    model_config = ConfigDict(extra="allow")

    # Required fields for all table types
    name: str = Field(..., description="Table name")
    schema: str = Field(..., description="Schema name")
    database: str = Field(..., description="Database name")
    columns: List[ColumnSpec] = Field(..., description="Column specifications")

    # Common optional fields
    comment: Optional[str] = Field(None, description="Table comment")
    change_tracking: bool = Field(
        False, description="Whether to enable change tracking")
    data_retention_time_in_days: Optional[int] = Field(
        None, description="Data retention time in days")
    cluster_by: Optional[List[str]] = Field(
        None, description="Columns to cluster by")

    # Type identification field
    table_type: str = Field(..., description="Table type identifier")

    # Environment prefixing
    prefix_with_environment: Optional[bool] = Field(
        True, description="Whether to prefix table and schema names with environment")

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        """Validate table name."""
        if not v:
            raise ValueError("Table name cannot be empty")
        return v

    @field_validator('schema')
    @classmethod
    def validate_schema(cls, v):
        """Validate schema name."""
        if not v:
            raise ValueError("Schema name cannot be empty")
        return v

    @field_validator('columns')
    @classmethod
    def validate_columns(cls, v):
        """Validate columns."""
        if not v:
            raise ValueError("Table must have at least one column")
        return v

    @field_validator('data_retention_time_in_days')
    @classmethod
    def validate_retention_days(cls, v):
        """Validate retention days."""
        if v is not None and v < 0:
            raise ValueError("data_retention_time_in_days must be >= 0")
        return v


class StandardTableSpec(BaseTableSpec):
    """Standard table specification."""

    table_type: Literal["STANDARD"] = Field(
        "STANDARD", description="Standard table type")


class DynamicTableSpec(BaseTableSpec):
    """Dynamic table specification."""

    table_type: Literal["DYNAMIC"] = Field(
        "DYNAMIC", description="Dynamic table type")

    # Required fields
    query: str = Field(..., description="Query for the dynamic table")
    target_lag: str = Field(...,
                            description="The target lag time, e.g. '5 minutes'")
    warehouse: str = Field(..., description="Warehouse to use for refreshes")

    # Optional fields with defaults
    refresh_mode: Literal["AUTO", "MANUAL", "SCHEDULED"] = Field(
        "AUTO", description="Refresh mode")
    refresh_schedule: Optional[str] = Field(
        None, description="Cron-like schedule for refreshes")
    initialize: bool = Field(
        True, description="Whether to initialize the table with data")
    kind: str = Field('PERMANENT', description="Table kind")
    max_data_extension_time_in_days: Optional[int] = Field(
        None, description="Maximum data extension time in days")

    @model_validator(mode='after')
    def validate_dynamic_requirements(self):
        """Validate dynamic table requirements."""
        if not self.query:
            raise ValueError("Dynamic tables require a query")

        if not self.cluster_by:
            raise ValueError("DYNAMIC tables require clustering")

        return self

    def to_snowflake(self) -> Dict[str, Any]:
        """Convert to Snowflake dynamic table configuration."""
        config = {
            'name': self.name,
            'target_lag': self.target_lag,
            'warehouse': self.warehouse,
            'query': self.query,
            'kind': self.kind,
            'refresh_mode': self.refresh_mode,
            'initialize': self.initialize
        }

        # Add optional fields if they are set
        if self.cluster_by:
            config['cluster_by'] = self.cluster_by
        if self.comment:
            config['comment'] = self.comment
        if self.data_retention_time_in_days:
            config['data_retention_time_in_days'] = self.data_retention_time_in_days
        if self.max_data_extension_time_in_days:
            config['max_data_extension_time_in_days'] = self.max_data_extension_time_in_days

        return config


class HybridTableSpec(BaseTableSpec):
    """Hybrid table specification."""

    table_type: Literal["HYBRID"] = Field(
        "HYBRID", description="Hybrid table type")

    @model_validator(mode='after')
    def validate_hybrid_requirements(self):
        """Validate hybrid table requirements."""
        # Check for primary key constraint
        has_primary_key = any(col.primary_key for col in self.columns)
        if not has_primary_key:
            raise ValueError("HYBRID tables require a primary key constraint")

        return self


# Union type for all table specs
TableSpec = Annotated[
    Union[StandardTableSpec, DynamicTableSpec, HybridTableSpec],
    Field(discriminator="table_type")
]
