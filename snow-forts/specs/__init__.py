"""Specification definitions for Snowflake resources."""

from .role import RoleSpec
from .user import UserSpec
from .warehouse import WarehouseSpec
from .database import DatabaseSpec

__all__ = [
    'RoleSpec',
    'UserSpec',
    'WarehouseSpec',
    'DatabaseSpec'
]
