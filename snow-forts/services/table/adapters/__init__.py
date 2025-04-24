"""Table adapters for different table types."""

from .base import TableAdapter
from .standard import StandardTableAdapter
from .hybrid import HybridTableAdapter
from .dynamic import DynamicTableAdapter

__all__ = [
    "TableAdapter",
    "StandardTableAdapter",
    "HybridTableAdapter",
    "DynamicTableAdapter"
]
