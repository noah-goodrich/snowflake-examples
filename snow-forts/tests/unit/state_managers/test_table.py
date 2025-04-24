"""Tests for the TableStateManager."""

import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from snowflake.core import Root
from snowflake.core.table import Table as SnowflakeTable, TableColumn

from state_managers.table import TableStateManager, TableState


@pytest.fixture
def mock_snow():
    """Create a mock Snowflake connection."""
    mock = Mock(spec=Root)
    mock.cursor = Mock()
    return mock


@pytest.fixture
def mock_table():
    """Create a mock Snowflake table."""
    table = Mock(spec=SnowflakeTable)
    table.name = "test_table"
    table.schema_name = "test_schema"
    table.database_name = "test_db"
    table.comment = "Test table"
    table.columns = [
        TableColumn(name="id", datatype="NUMBER", nullable=False),
        TableColumn(name="name", datatype="VARCHAR", nullable=True)
    ]
    return table


@pytest.fixture
def table_state_manager(mock_snow):
    """Create a TableStateManager instance."""
    return TableStateManager(mock_snow)


def test_apply_create(table_state_manager, mock_table):
    """Test applying state for a new table."""
    # Mock current state as None (table doesn't exist)
    table_state_manager._get_current_state = Mock(return_value=None)

    # Mock service operations
    table_state_manager.service.create = Mock(
        return_value={"status": "success"})

    # Apply state
    result = table_state_manager.apply(mock_table)

    # Verify results
    assert result == {"create": {"status": "success"}}
    table_state_manager.service.create.assert_called_once_with(mock_table)


def test_apply_alter(table_state_manager, mock_table):
    """Test applying state for an existing table that needs alteration."""
    # Create current state
    current_state = TableState(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[
            TableColumn(name="id", datatype="NUMBER", nullable=False),
            TableColumn(name="old_name", datatype="VARCHAR", nullable=True)
        ],
        comment="Old comment",
        created_at=datetime.now(),
        last_altered=datetime.now()
    )

    # Mock current state
    table_state_manager._get_current_state = Mock(return_value=current_state)

    # Mock service operations
    table_state_manager.service.alter = Mock(
        return_value={"status": "success"})

    # Apply state
    result = table_state_manager.apply(mock_table)

    # Verify results
    assert result == {"alter": {"status": "success"}}
    table_state_manager.service.alter.assert_called_once_with(mock_table)


def test_apply_no_changes(table_state_manager, mock_table):
    """Test applying state when no changes are needed."""
    # Create current state matching desired state
    current_state = TableState(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=mock_table.columns,
        comment="Test table",
        created_at=datetime.now(),
        last_altered=datetime.now()
    )

    # Mock current state
    table_state_manager._get_current_state = Mock(return_value=current_state)

    # Apply state
    result = table_state_manager.apply(mock_table)

    # Verify no operations were performed
    assert result == {}


def test_get_current_state(table_state_manager, mock_table):
    """Test getting current table state."""
    # Mock table info
    mock_table_info = Mock()
    mock_table_info.name = "test_table"
    mock_table_info.schema_name = "test_schema"
    mock_table_info.database_name = "test_db"
    mock_table_info.columns = mock_table.columns
    mock_table_info.comment = "Test table"
    mock_table_info.created_on = datetime.now()
    mock_table_info.last_altered = datetime.now()

    # Mock database access
    mock_db = Mock()
    mock_schema = Mock()
    mock_schema.tables = {mock_table.name: mock_table_info}
    mock_db.schemas = {mock_table.schema_name: mock_schema}
    table_state_manager.snow.databases = {mock_table.database_name: mock_db}

    # Mock statistics
    mock_cursor = Mock()
    mock_cursor.fetchone.return_value = {"ROW_COUNT": 100, "BYTES": 1024}
    table_state_manager.snow.cursor.return_value = mock_cursor

    # Get current state
    state = table_state_manager._get_current_state(mock_table)

    # Verify state
    assert state is not None
    assert state.name == "test_table"
    assert state.schema == "test_schema"
    assert state.database == "test_db"
    assert len(state.columns) == 2
    assert state.comment == "Test table"
    assert state.row_count == 100
    assert state.bytes == 1024


def test_get_current_state_not_found(table_state_manager, mock_table):
    """Test getting current state for non-existent table."""
    # Mock database access to raise exception
    mock_db = Mock()
    mock_schema = Mock()
    mock_schema.tables = {}
    mock_db.schemas = {mock_table.schema_name: mock_schema}
    table_state_manager.snow.databases = {mock_table.database_name: mock_db}

    # Get current state
    state = table_state_manager._get_current_state(mock_table)

    # Verify state is None
    assert state is None


def test_needs_alter_comment_change(table_state_manager):
    """Test detecting need for alter when comment changes."""
    # Create current state
    current = TableState(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[],
        comment="Old comment"
    )

    # Create desired table with different comment
    desired = Mock(spec=SnowflakeTable)
    desired.comment = "New comment"
    desired.columns = []

    # Check if alter is needed
    assert table_state_manager._needs_alter(current, desired)


def test_needs_alter_column_change(table_state_manager):
    """Test detecting need for alter when columns change."""
    # Create current state
    current = TableState(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[
            TableColumn(name="id", datatype="NUMBER", nullable=False),
            TableColumn(name="name", datatype="VARCHAR", nullable=True)
        ]
    )

    # Create desired table with different columns
    desired = Mock(spec=SnowflakeTable)
    desired.comment = None
    desired.columns = [
        TableColumn(name="id", datatype="NUMBER", nullable=False),
        TableColumn(name="new_name", datatype="VARCHAR", nullable=True)
    ]

    # Check if alter is needed
    assert table_state_manager._needs_alter(current, desired)


def test_needs_alter_no_changes(table_state_manager):
    """Test detecting no need for alter when no changes."""
    # Create current state
    current = TableState(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[
            TableColumn(name="id", datatype="NUMBER", nullable=False),
            TableColumn(name="name", datatype="VARCHAR", nullable=True)
        ],
        comment="Test table"
    )

    # Create desired table matching current state
    desired = Mock(spec=SnowflakeTable)
    desired.comment = "Test table"
    desired.columns = [
        TableColumn(name="id", datatype="NUMBER", nullable=False),
        TableColumn(name="name", datatype="VARCHAR", nullable=True)
    ]

    # Check if alter is needed
    assert not table_state_manager._needs_alter(current, desired)
