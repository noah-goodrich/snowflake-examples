"""Role specification definitions."""

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class RoleSpec:
    """Specification for a Snowflake role."""

    name: str
    comment: Optional[str] = None
    granted_roles: Optional[List[str]] = None
    granted_to: Optional[List[str]] = None
    prefix_with_environment: bool = True
