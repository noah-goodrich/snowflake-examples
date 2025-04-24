"""Tests for the TableService class."""

import pytest
from unittest.mock import MagicMock, patch
from snowflake.core import Root
from snowflake.core.table import Table as SnowflakeTable

from services.table import TableService
from specs.table import (
    StandardTableSpec,
    HybridTableSpec,
    DynamicTableSpec,
    ColumnSpec
)


@pytest.fixture
def mock_root():
    """Create a mock Snowflake root object."""
    return MagicMock(spec=Root)


@pytest.fixture
def table_service(mock_root):
    """Create a TableService instance with a mock root."""
    return TableService(mock_root)


@pytest.fixture
def standard_spec():
    """Create a standard table specification."""
    return StandardTableSpec(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[
            ColumnSpec(name="id", datatype="INTEGER", nullable=False),
            ColumnSpec(name="name", datatype="VARCHAR(100)", nullable=True)
        ]
    )


@pytest.fixture
def hybrid_spec():
    """Create a hybrid table specification."""
    return HybridTableSpec(
        name="test_hybrid",
        schema="test_schema",
        database="test_db",
        columns=[
            ColumnSpec(name="id", datatype="INTEGER",
                       nullable=False, primary_key=True),
            ColumnSpec(name="name", datatype="VARCHAR(100)", nullable=True)
        ]
    )


@pytest.fixture
def dynamic_spec():
    """Create a dynamic table specification."""
    return DynamicTableSpec(
        name="test_dynamic",
        schema="test_schema",
        database="test_db",
        columns=[
            ColumnSpec(name="id", datatype="INTEGER", nullable=False),
            ColumnSpec(name="name", datatype="VARCHAR(100)", nullable=True)
        ],
        target_lag="1 minute",
        warehouse="test_warehouse",
        query="SELECT * FROM test_source",
        cluster_by=["id"],
        comment="Test dynamic table",
        data_retention_time_in_days=7,
        max_data_extension_time_in_days=7,
        refresh_mode="AUTO",
        initialize=True
    )


def test_create_standard(table_service, standard_spec, mock_root):
    """Test creating a standard table."""
    with patch.object(TableService, 'create') as mock_create:
        mock_create.return_value = True
        result = table_service.create(standard_spec)
        assert result is True
        mock_create.assert_called_once_with(standard_spec)


def test_create_hybrid(table_service, hybrid_spec, mock_root):
    """Test creating a hybrid table."""
    with patch.object(TableService, 'create') as mock_create:
        mock_create.return_value = True
        result = table_service.create(hybrid_spec)
        assert result is True
        mock_create.assert_called_once_with(hybrid_spec)


def test_create_dynamic(table_service, dynamic_spec, mock_root):
    """Test creating a dynamic table."""
    with patch.object(TableService, 'create') as mock_create:
        mock_create.return_value = True
        result = table_service.create(dynamic_spec)
        assert result is True
        mock_create.assert_called_once_with(dynamic_spec)


def test_alter(table_service, standard_spec, mock_root):
    """Test altering a table."""
    with patch.object(TableService, 'alter') as mock_alter:
        mock_alter.return_value = True
        result = table_service.alter(standard_spec)
        assert result is True
        mock_alter.assert_called_once_with(standard_spec)


def test_drop(table_service, standard_spec, mock_root):
    """Test dropping a table."""
    with patch.object(TableService, 'drop') as mock_drop:
        mock_drop.return_value = True
        result = table_service.drop(standard_spec)
        assert result is True
        mock_drop.assert_called_once_with(standard_spec)


def test_get(table_service, standard_spec, mock_root):
    """Test getting a table."""
    mock_table = MagicMock(spec=SnowflakeTable)
    with patch.object(TableService, 'get') as mock_get:
        mock_get.return_value = mock_table
        result = table_service.get(standard_spec)
        assert result == mock_table
        mock_get.assert_called_once_with(standard_spec)


def test_exists(table_service, standard_spec, mock_root):
    """Test checking if a table exists."""
    with patch.object(TableService, 'exists') as mock_exists:
        mock_exists.return_value = True
        result = table_service.exists(standard_spec)
        assert result is True
        mock_exists.assert_called_once_with(standard_spec)


def test_get_columns(table_service, standard_spec, mock_root):
    """Test getting table columns."""
    mock_columns = [
        ColumnSpec(name="id", datatype="INTEGER", nullable=False),
        ColumnSpec(name="name", datatype="VARCHAR(100)", nullable=True)
    ]
    with patch.object(TableService, 'get_columns') as mock_get_columns:
        mock_get_columns.return_value = mock_columns
        result = table_service.get_columns(standard_spec)
        assert result == mock_columns
        mock_get_columns.assert_called_once_with(standard_spec)


def test_create_error_handling(table_service, standard_spec, mock_root):
    """Test error handling when creating a table."""
    with patch('services.table.adapters.standard.StandardTableAdapter.create') as mock_create:
        mock_create.side_effect = Exception("Test error")
        result = table_service.create(standard_spec)
        assert result is False


def test_alter_error_handling(table_service, standard_spec, mock_root):
    """Test error handling when altering a table."""
    with patch('services.table.adapters.standard.StandardTableAdapter.alter') as mock_alter:
        mock_alter.side_effect = Exception("Test error")
        result = table_service.alter(standard_spec)
        assert result is False


def test_drop_error_handling(table_service, standard_spec, mock_root):
    """Test error handling when dropping a table."""
    with patch('services.table.adapters.standard.StandardTableAdapter.drop') as mock_drop:
        mock_drop.side_effect = Exception("Test error")
        result = table_service.drop(standard_spec)
        assert result is False


def test_get_error_handling(table_service, standard_spec, mock_root):
    """Test error handling when getting a table."""
    with patch('services.table.adapters.standard.StandardTableAdapter.get') as mock_get:
        mock_get.side_effect = Exception("Test error")
        result = table_service.get(standard_spec)
        assert result is None


def test_exists_error_handling(table_service, standard_spec, mock_root):
    """Test error handling when checking table existence."""
    with patch('services.table.adapters.standard.StandardTableAdapter.exists') as mock_exists:
        mock_exists.side_effect = Exception("Test error")
        result = table_service.exists(standard_spec)
        assert result is False


def test_get_columns_error_handling(table_service, standard_spec, mock_root):
    """Test error handling when getting table columns."""
    with patch('services.table.adapters.standard.StandardTableAdapter.get') as mock_get:
        mock_get.side_effect = Exception("Test error")
        result = table_service.get_columns(standard_spec)
        assert result == []
