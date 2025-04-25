"""Warehouse specification definitions."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class WarehouseSpec:
    """Specification for a Snowflake warehouse."""

    name: str
    size: str
    auto_suspend: int
    auto_resume: bool
    comment: Optional[str] = None
    prefix_with_environment: bool = True
