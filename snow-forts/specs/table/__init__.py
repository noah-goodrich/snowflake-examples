"""Table specifications for Snowflake tables."""

from typing import Union
from .specs import (
    BaseTableSpec,
    ColumnSpec,
    ColumnOperation,
    StandardTableSpec,
    HybridTableSpec,
    DynamicTableSpec,
    TableSpec
)

__all__ = [
    'BaseTableSpec',
    'ColumnSpec',
    'ColumnOperation',
    'StandardTableSpec',
    'HybridTableSpec',
    'DynamicTableSpec',
    'TableSpec'
]
