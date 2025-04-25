"""Tests for table state operations."""

import pytest
from unittest.mock import Mock, patch

from resources.table.specs import (
    ColumnSpec,
    StandardTableSpec
)
from resources.table.state import (
    CreateTableOperation,
    AlterTableOperation,
    DropTableOperation,
    TableStateChange
)


@pytest.fixture
def mock_table_resource():
    """Mock table resource."""
    mock = Mock()
    mock.create.return_value = Mock(name="test_table")
    mock.exists.return_value = False
    mock.get.return_value = None
    return mock


@pytest.fixture
def table_spec():
    """Standard table specification."""
    return StandardTableSpec(
        name="test_table",
        schema="test_schema",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table",
        cluster_by=["id"],
        change_tracking=False,
        data_retention_time_in_days=1
    )


@pytest.fixture
def mock_metadata():
    """Mock metadata for state changes."""
    return {
        "deployed_by": "test_user",
        "deployment_id": "test_deployment"
    }


class TestCreateTableOperation:
    """Tests for CreateTableOperation."""

    def test_apply_creates_nonexistent_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operation to create a nonexistent table."""
        # Setup
        mock_table_resource.exists.return_value = False

        # Create operation
        operation = CreateTableOperation(table_spec)

        # Apply
        result = operation.apply(mock_table_resource, mock_metadata)

        # Assert
        assert result.successful is True
        assert result.created is True
        mock_table_resource.create.assert_called_once_with(table_spec)

    def test_apply_skips_existing_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operation skips an existing table."""
        # Setup
        mock_table_resource.exists.return_value = True

        # Create operation
        operation = CreateTableOperation(table_spec)

        # Apply
        result = operation.apply(mock_table_resource, mock_metadata)

        # Assert
        assert result.successful is True
        assert result.created is False
        mock_table_resource.create.assert_not_called()

    def test_rollback_drops_created_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test rolling back drops a created table."""
        # Setup
        mock_table_resource.exists.return_value = True

        # Create operation
        operation = CreateTableOperation(table_spec)

        # Create result with created=True
        from resources.table.state import OperationResult
        result = OperationResult(
            operation=operation,
            successful=True,
            created=True,
            metadata=mock_metadata
        )

        # Rollback
        operation.rollback(mock_table_resource, result)

        # Assert
        mock_table_resource.drop.assert_called_once_with(
            table_spec.name, table_spec.schema)

    def test_rollback_skips_noncreated_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test rolling back skips a non-created table."""
        # Setup
        mock_table_resource.exists.return_value = True

        # Create operation
        operation = CreateTableOperation(table_spec)

        # Create result with created=False
        from resources.table.state import OperationResult
        result = OperationResult(
            operation=operation,
            successful=True,
            created=False,
            metadata=mock_metadata
        )

        # Rollback
        operation.rollback(mock_table_resource, result)

        # Assert
        mock_table_resource.drop.assert_not_called()


class TestAlterTableOperation:
    """Tests for AlterTableOperation."""

    def test_apply_alters_existing_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operation to alter an existing table."""
        # Setup
        mock_table_resource.exists.return_value = True
        mock_table = Mock(name="test_table")
        mock_table_resource.get.return_value = mock_table
        mock_table_resource.alter.return_value = mock_table

        # Create operation
        operation = AlterTableOperation(table_spec)

        # Apply
        result = operation.apply(mock_table_resource, mock_metadata)

        # Assert
        assert result.successful is True
        mock_table_resource.alter.assert_called_once_with(table_spec)

    def test_apply_fails_for_nonexistent_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operation fails for nonexistent table."""
        # Setup
        mock_table_resource.exists.return_value = False

        # Create operation
        operation = AlterTableOperation(table_spec)

        # Apply
        result = operation.apply(mock_table_resource, mock_metadata)

        # Assert
        assert result.successful is False
        mock_table_resource.alter.assert_not_called()

    def test_rollback_no_op(self, mock_table_resource, table_spec, mock_metadata):
        """Test rolling back is a no-op."""
        # Setup
        mock_table_resource.exists.return_value = True

        # Create operation
        operation = AlterTableOperation(table_spec)

        # Create result
        from resources.table.state import OperationResult
        result = OperationResult(
            operation=operation,
            successful=True,
            metadata=mock_metadata
        )

        # Rollback
        operation.rollback(mock_table_resource, result)

        # Assert - no operations should be called
        mock_table_resource.alter.assert_not_called()
        mock_table_resource.drop.assert_not_called()
        mock_table_resource.create.assert_not_called()


class TestDropTableOperation:
    """Tests for DropTableOperation."""

    def test_apply_drops_existing_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operation to drop an existing table."""
        # Setup
        mock_table_resource.exists.return_value = True
        mock_table = Mock(name="test_table")
        mock_table_resource.get.return_value = mock_table

        # Create operation
        operation = DropTableOperation(table_spec.name, table_spec.schema)

        # Apply
        result = operation.apply(mock_table_resource, mock_metadata)

        # Assert
        assert result.successful is True
        assert result.dropped is True
        assert result.table_spec is not None
        mock_table_resource.drop.assert_called_once_with(
            table_spec.name, table_spec.schema)

    def test_apply_skips_nonexistent_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operation skips a nonexistent table."""
        # Setup
        mock_table_resource.exists.return_value = False

        # Create operation
        operation = DropTableOperation(table_spec.name, table_spec.schema)

        # Apply
        result = operation.apply(mock_table_resource, mock_metadata)

        # Assert
        assert result.successful is True
        assert result.dropped is False
        mock_table_resource.drop.assert_not_called()

    def test_rollback_recreates_dropped_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test rolling back recreates a dropped table."""
        # Setup
        mock_table_resource.exists.return_value = False

        # Create operation
        operation = DropTableOperation(table_spec.name, table_spec.schema)

        # Create result with dropped=True and saved table spec
        from resources.table.state import OperationResult
        result = OperationResult(
            operation=operation,
            successful=True,
            dropped=True,
            table_spec=table_spec,
            metadata=mock_metadata
        )

        # Rollback
        operation.rollback(mock_table_resource, result)

        # Assert
        mock_table_resource.create.assert_called_once_with(table_spec)

    def test_rollback_skips_nondropped_table(self, mock_table_resource, table_spec, mock_metadata):
        """Test rolling back skips a non-dropped table."""
        # Setup
        mock_table_resource.exists.return_value = True

        # Create operation
        operation = DropTableOperation(table_spec.name, table_spec.schema)

        # Create result with dropped=False
        from resources.table.state import OperationResult
        result = OperationResult(
            operation=operation,
            successful=True,
            dropped=False,
            metadata=mock_metadata
        )

        # Rollback
        operation.rollback(mock_table_resource, result)

        # Assert
        mock_table_resource.create.assert_not_called()


class TestTableStateChange:
    """Tests for TableStateChange."""

    def test_check_current_state(self, mock_table_resource, table_spec):
        """Test checking current state of tables."""
        # Setup - one table exists, one doesn't
        def exists_side_effect(name, schema):
            return name == "existing_table"

        mock_table_resource.exists.side_effect = exists_side_effect

        # Create state change
        state_change = TableStateChange([
            CreateTableOperation(table_spec),
            DropTableOperation("existing_table", "test_schema")
        ])

        # Check current state
        current_state = state_change.check_current_state(mock_table_resource)

        # Assert
        assert len(current_state) == 2
        assert current_state[0]["exists"] is False
        assert current_state[0]["name"] == "test_table"
        assert current_state[0]["schema"] == "test_schema"

        assert current_state[1]["exists"] is True
        assert current_state[1]["name"] == "existing_table"
        assert current_state[1]["schema"] == "test_schema"

    def test_apply_operations(self, mock_table_resource, table_spec, mock_metadata):
        """Test applying operations in a state change."""
        # Setup
        def exists_side_effect(name, schema):
            return name == "existing_table"

        mock_table_resource.exists.side_effect = exists_side_effect

        # Create state change
        state_change = TableStateChange([
            CreateTableOperation(table_spec),
            DropTableOperation("existing_table", "test_schema")
        ])

        # Apply operations
        results = state_change.apply_operations(
            mock_table_resource, mock_metadata)

        # Assert
        assert len(results) == 2
        assert results[0].successful is True
        assert results[0].created is True

        assert results[1].successful is True
        assert results[1].dropped is True

    def test_rollback_operations(self, mock_table_resource, table_spec, mock_metadata):
        """Test rolling back operations in a state change."""
        # Setup
        mock_table_resource.exists.return_value = True

        # Create operations
        create_op = CreateTableOperation(table_spec)
        drop_op = DropTableOperation("existing_table", "test_schema")

        # Create state change
        state_change = TableStateChange([create_op, drop_op])

        # Create results to roll back
        from resources.table.state import OperationResult
        results = [
            OperationResult(
                operation=create_op,
                successful=True,
                created=True,
                metadata=mock_metadata
            ),
            OperationResult(
                operation=drop_op,
                successful=True,
                dropped=True,
                table_spec=StandardTableSpec(
                    name="existing_table",
                    schema="test_schema",
                    columns=[ColumnSpec(name="id", datatype="NUMBER")]
                ),
                metadata=mock_metadata
            )
        ]

        # Rollback operations
        state_change.rollback_operations(mock_table_resource, results)

        # Assert - should roll back in reverse order
        assert mock_table_resource.create.call_count == 1
        assert mock_table_resource.drop.call_count == 1
