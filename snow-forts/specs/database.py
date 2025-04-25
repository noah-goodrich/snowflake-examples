"""Database specification definitions."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class DatabaseSpec:
    """Specification for a Snowflake database."""

    name: str
    schemas: List[str]
    comment: Optional[str] = None
    prefix_with_environment: bool = True
