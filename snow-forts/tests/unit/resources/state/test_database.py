"""Tests for database state management."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from snowflake.core import Root
from resources.database import Database, DatabaseConfig
from resources.state.database import (
    DatabaseStateChange,
    CreateDatabaseOperation,
    AlterDatabaseOperation,
    DropDatabaseOperation
)
from resources.state.base import StateChangeMetadata, OperationType


@pytest.fixture
def database_config():
    """Create a database configuration."""
    return DatabaseConfig(
        name="TEST_DB",
        comment="Test database"
    )


@pytest.fixture
def database_state_change(snow):
    """Create a database state change instance with persistence mocked."""
    with patch('resources.state.database.StatePersistence') as mock_persistence:
        state_change = DatabaseStateChange(snow)
        return state_change


def test_create_database_operation(mock_database, database_config):
    """Test database creation operation."""
    metadata = StateChangeMetadata("test_id", "test description")
    operation = CreateDatabaseOperation(
        mock_database, database_config, metadata)

    # Test validation when database doesn't exist
    mock_database.exists.return_value = False
    assert operation.validate() is True

    # Test validation when database exists
    mock_database.exists.return_value = True
    assert operation.validate() is False

    # Test apply
    mock_database.exists.return_value = False
    operation.apply()
    mock_database.create.assert_called_once_with(database_config)

    # Test rollback
    mock_database.exists.return_value = True
    operation.rollback()
    mock_database.drop.assert_called_once_with(
        database_config.name, cascade=True)


def test_alter_database_operation(mock_database, database_config):
    """Test database alteration operation."""
    metadata = StateChangeMetadata("test_id", "test description")
    operation = AlterDatabaseOperation(
        mock_database, database_config, metadata)

    # Test validation when database exists
    mock_database.exists.return_value = True
    assert operation.validate() is True

    # Test validation when database doesn't exist
    mock_database.exists.return_value = False
    assert operation.validate() is False

    # Test apply
    mock_database.exists.return_value = True
    operation.apply()
    mock_database.alter.assert_called_once_with(
        database_config.name, database_config)

    # Test rollback
    mock_database.exists.return_value = True
    operation.rollback()
    mock_database.alter.assert_called_with(
        database_config.name,
        DatabaseConfig(
            name=database_config.name,
            comment=database_config.comment
        )
    )


def test_drop_database_operation(mock_database, database_config):
    """Test database drop operation."""
    metadata = StateChangeMetadata("test_id", "test description")
    operation = DropDatabaseOperation(mock_database, database_config, metadata)

    # Test validation when database exists
    mock_database.exists.return_value = True
    assert operation.validate() is True

    # Test validation when database doesn't exist
    mock_database.exists.return_value = False
    assert operation.validate() is False

    # Test apply
    mock_database.exists.return_value = True
    operation.apply()
    mock_database.drop.assert_called_once_with(
        database_config.name, cascade=True)

    # Test rollback
    mock_database.exists.return_value = False
    operation.rollback()
    mock_database.create.assert_called_once_with(database_config)


def test_database_state_change_check_current_state(database_state_change, mock_database):
    """Test checking current database state."""
    current_state = database_state_change.check_current_state(mock_database)
    assert current_state == {
        "name": mock_database.name,
        "comment": mock_database.comment
    }


def test_database_state_change_validate_state(database_state_change):
    """Test database state validation."""
    # Valid state
    valid_state = DatabaseConfig(
        name="test_db",
        comment="Test database"
    )
    assert database_state_change.validate_state(valid_state) is True

    # Invalid state - empty name
    invalid_state = DatabaseConfig(name="")
    assert database_state_change.validate_state(invalid_state) is False

    # Invalid state - not a DatabaseConfig
    assert database_state_change.validate_state({"name": "test"}) is False


def test_database_state_change_get_operation(database_state_change, mock_database, database_config):
    """Test getting appropriate database operation."""
    metadata = StateChangeMetadata("test_id", "test description")

    # Test getting create operation
    mock_database.exists.return_value = False
    operation = database_state_change.get_operation(
        mock_database, database_config, metadata)
    assert isinstance(operation, CreateDatabaseOperation)

    # Test getting alter operation
    mock_database.exists.return_value = True
    operation = database_state_change.get_operation(
        mock_database, database_config, metadata)
    assert isinstance(operation, AlterDatabaseOperation)

    # Test getting drop operation
    operation = database_state_change.get_operation(
        mock_database, None, metadata)
    assert isinstance(operation, DropDatabaseOperation)
