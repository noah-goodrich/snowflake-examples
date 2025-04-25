"""Table service tests."""

from typing import Dict, List, Optional, Any
from unittest.mock import Mock, patch
from datetime import datetime
import pytest

from snowflake.core import Root
from snowflake.core.table import Table as SnowflakeTable

from services.table import TableService
from state_managers.table import TableStateManager
from state_managers.types import StateChangeMetadata

from specs.table import (
    ColumnSpec,
    ColumnOperation,
    StandardTableSpec,
    HybridTableSpec,
    DynamicTableSpec
)

from services.table.adapters import (
    TableAdapter,
    StandardTableAdapter,
    HybridTableAdapter,
    DynamicTableAdapter
)
from state_managers.table import (
    CreateTableOperation,
    AlterTableOperation,
    DropTableOperation
)


@pytest.fixture
def mock_snow():
    """Mock Snowflake root object."""
    mock = Mock()
    mock.databases = {}
    mock.sql = Mock()
    mock.sql.return_value.collect.return_value = []
    return mock


@pytest.fixture
def mock_table():
    """Mock Snowflake table object."""
    mock = Mock()
    mock.name = "test_table"
    mock.schema = "test_schema"
    mock.drop = Mock()
    mock.alter = Mock()
    return mock


@pytest.fixture
def mock_adapter():
    """Mock table adapter."""
    mock = Mock(spec=TableAdapter)
    mock.create.return_value = Mock(name="test_table")
    mock.get.return_value = {
        "name": "test_table", "schema_name": "test_schema"}
    mock.exists.return_value = True
    return mock


@pytest.fixture
def standard_spec():
    """Standard table specification."""
    return StandardTableSpec(
        name="test_table",
        schema="test_schema",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table"
    )


@pytest.fixture
def hybrid_spec():
    """Hybrid table specification."""
    return HybridTableSpec(
        name="test_table",
        schema="test_schema",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table",
        cluster_by=["id"],
        auto_clustering=True
    )


@pytest.fixture
def dynamic_spec():
    """Dynamic table specification."""
    return DynamicTableSpec(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table",
        cluster_by=["id"],
        query="SELECT * FROM source_table",
        target_lag="10 minutes",
        warehouse="TEST_WH",
        refresh_mode="AUTO",
        initialize=True
    )


class TestTableService:
    """Tests for TableService."""

    def test_format_name_with_prefix(self):
        """Test formatting a name with an environment prefix."""
        service = TableService(Mock(), "dev")
        result = service._format_name("test_name")
        assert result == "dev_test_name"

    def test_format_name_without_prefix(self):
        """Test formatting a name without an environment prefix."""
        service = TableService(Mock())
        result = service._format_name("test_name")
        assert result == "test_name"

    def test_get_adapter_standard(self, mock_snow, standard_spec):
        """Test getting the appropriate adapter for a standard table spec."""
        service = TableService(mock_snow)

        with patch('services.table.adapters.TableAdapter.create_for_spec') as mock_create:
            mock_create.return_value = Mock(spec=StandardTableAdapter)
            adapter = service._get_adapter(standard_spec)

            mock_create.assert_called_once_with(
                mock_snow,
                standard_spec,
                "test_table",
                "test_schema"
            )
            assert adapter is not None

    def test_get_adapter_hybrid(self, mock_snow, hybrid_spec):
        """Test getting the appropriate adapter for a hybrid table spec."""
        service = TableService(mock_snow)

        with patch('services.table.adapters.TableAdapter.create_for_spec') as mock_create:
            mock_create.return_value = Mock(spec=HybridTableAdapter)
            adapter = service._get_adapter(hybrid_spec)

            mock_create.assert_called_once_with(
                mock_snow,
                hybrid_spec,
                "test_table",
                "test_schema"
            )
            assert adapter is not None

    def test_get_adapter_dynamic(self, mock_snow, dynamic_spec):
        """Test getting the appropriate adapter for a dynamic table spec."""
        service = TableService(mock_snow)

        with patch('services.table.adapters.TableAdapter.create_for_spec') as mock_create:
            mock_create.return_value = Mock(spec=DynamicTableAdapter)
            adapter = service._get_adapter(dynamic_spec)

            mock_create.assert_called_once_with(
                mock_snow,
                dynamic_spec,
                "test_table",
                "test_schema"
            )
            assert adapter is not None

    def test_create_table(self, mock_snow, standard_spec, mock_adapter):
        """Test creating a table."""
        service = TableService(mock_snow)

        # Mock the adapter and state manager
        with patch('services.table.TableService._get_adapter') as mock_get_adapter, \
                patch.object(service.state_manager, 'apply') as mock_apply:

            mock_get_adapter.return_value = mock_adapter

            # Create a mock result
            mock_result = OperationResult(
                operation=Mock(spec=CreateTableOperation),
                successful=True,
                created=True,
                metadata={"status": "created"}
            )
            mock_apply.return_value = mock_result

            # Call the method
            result = service.create_table(standard_spec)

            # Verify
            mock_get_adapter.assert_called_once_with(standard_spec)
            mock_apply.assert_called_once()
            assert result == {"status": "created"}

    def test_alter_table(self, mock_snow, standard_spec, mock_adapter):
        """Test altering a table."""
        service = TableService(mock_snow)

        # Mock the adapter and state manager
        with patch('services.table.TableService._get_adapter') as mock_get_adapter, \
                patch.object(service.state_manager, 'apply') as mock_apply:

            mock_get_adapter.return_value = mock_adapter

            # Create a mock result
            mock_result = OperationResult(
                operation=Mock(spec=AlterTableOperation),
                successful=True,
                created=False,
                metadata={"status": "altered"}
            )
            mock_apply.return_value = mock_result

            # Define column operations
            column_ops = [
                ColumnOperation(
                    operation="ADD",
                    column_name="new_column",
                    datatype="VARCHAR"
                )
            ]

            # Call the method
            result = service.alter_table(standard_spec, column_ops)

            # Verify
            mock_get_adapter.assert_called_once_with(standard_spec)
            mock_apply.assert_called_once()
            assert result == {"status": "altered"}

    def test_drop_table(self, mock_snow, standard_spec, mock_adapter):
        """Test dropping a table."""
        service = TableService(mock_snow)

        # Mock the adapter and state manager
        with patch('services.table.TableService._get_adapter') as mock_get_adapter, \
                patch.object(service.state_manager, 'apply') as mock_apply:

            mock_get_adapter.return_value = mock_adapter

            # Create a mock result
            mock_result = OperationResult(
                operation=Mock(spec=DropTableOperation),
                successful=True,
                dropped=True,
                metadata={"status": "dropped"}
            )
            mock_apply.return_value = mock_result

            # Call the method
            result = service.drop_table(standard_spec)

            # Verify
            mock_get_adapter.assert_called_once_with(standard_spec)
            mock_apply.assert_called_once()
            assert result == {"status": "dropped"}

    def test_get_table(self, mock_snow, standard_spec, mock_adapter):
        """Test getting a table."""
        service = TableService(mock_snow)

        # Mock the adapter
        with patch('services.table.TableService._get_adapter') as mock_get_adapter:
            mock_get_adapter.return_value = mock_adapter

            # Call the method
            result = service.get_table(standard_spec)

            # Verify
            mock_get_adapter.assert_called_once_with(standard_spec)
            mock_adapter.get.assert_called_once()
            assert result == {"name": "test_table",
                              "schema_name": "test_schema"}

    def test_table_exists(self, mock_snow, standard_spec, mock_adapter):
        """Test checking if a table exists."""
        service = TableService(mock_snow)

        # Mock the adapter
        with patch('services.table.TableService._get_adapter') as mock_get_adapter:
            mock_get_adapter.return_value = mock_adapter

            # Call the method
            result = service.table_exists(standard_spec)

            # Verify
            mock_get_adapter.assert_called_once_with(standard_spec)
            mock_adapter.exists.assert_called_once()
            assert result is True
