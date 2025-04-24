"""Unit tests for state managers."""

from typing import Dict, List, Optional, Any
from unittest.mock import Mock, patch
from datetime import datetime

from snowflake.core import Root
from snowflake.core.table import Table as SnowflakeTable, TableColumn

from state_managers.table import TableStateManager, TableState 