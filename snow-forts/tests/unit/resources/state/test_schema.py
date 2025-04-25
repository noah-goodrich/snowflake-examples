"""Tests for schema state management."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from snowflake.core import Root
from snowflake.core.schema import Schema
from resources.state.schema import (
    SchemaStateChange,
    CreateSchemaOperation,
    DropSchemaOperation
)
from resources.state.base import StateChangeMetadata, OperationType


@pytest.fixture
def mock_snow():
    """Create a mock Snowflake connection."""
    snow = MagicMock(spec=Root)
    snow.sql = MagicMock()  # Add sql method for StatePersistence
    return snow


@pytest.fixture
def mock_schema():
    """Create a mock schema."""
    schema = MagicMock()  # Remove spec to allow any method
    schema.name = "TEST_SCHEMA"
    schema.exists = MagicMock(return_value=False)
    schema.create = MagicMock()
    schema.drop = MagicMock()
    return schema


@pytest.fixture
def schema_state_change(mock_snow):
    """Create a schema state change instance with persistence mocked."""
    with patch('resources.state.schema.StatePersistence') as mock_persistence:
        state_change = SchemaStateChange(mock_snow)
        return state_change


def test_create_schema_operation(mock_schema):
    """Test schema creation operation."""
    schema_name = "SCHEMA3"
    metadata = StateChangeMetadata("test_id", "test description")
    operation = CreateSchemaOperation(mock_schema, schema_name, metadata)

    # Test validation when schema doesn't exist
    mock_schema.exists.return_value = False
    assert operation.validate() is True

    # Test validation when schema exists
    mock_schema.exists.return_value = True
    assert operation.validate() is False

    # Test apply
    mock_schema.exists.return_value = False
    operation.apply()
    mock_schema.create.assert_called_once_with(schema_name)

    # Test rollback
    mock_schema.exists.return_value = True
    operation.rollback()
    mock_schema.drop.assert_called_once_with(cascade=True)


def test_drop_schema_operation(mock_schema):
    """Test schema drop operation."""
    schema_name = "SCHEMA1"
    metadata = StateChangeMetadata("test_id", "test description")
    operation = DropSchemaOperation(mock_schema, schema_name, metadata)

    # Test validation when schema exists
    mock_schema.exists.return_value = True
    assert operation.validate() is True

    # Test validation when schema doesn't exist
    mock_schema.exists.return_value = False
    assert operation.validate() is False

    # Test apply
    mock_schema.exists.return_value = True
    operation.apply()
    mock_schema.drop.assert_called_once_with(cascade=True)

    # Test rollback
    mock_schema.exists.return_value = False
    operation.rollback()
    mock_schema.create.assert_called_once_with(schema_name)


def test_schema_state_change_check_current_state(schema_state_change, mock_schema):
    """Test checking current schema state."""
    current_state = schema_state_change.check_current_state(mock_schema)
    assert current_state == {"name": "TEST_SCHEMA"}


def test_schema_state_change_validate_state(schema_state_change):
    """Test schema state validation."""
    # Valid state
    assert schema_state_change.validate_state("SCHEMA3") is True

    # Invalid state - empty string
    assert schema_state_change.validate_state("") is False

    # Invalid state - not a string
    assert schema_state_change.validate_state(123) is False


def test_schema_state_change_get_operation(schema_state_change, mock_schema):
    """Test getting appropriate schema operation."""
    metadata = StateChangeMetadata("test_id", "test description")

    # Test getting create operation
    operation = schema_state_change.get_operation(
        mock_schema, "SCHEMA3", metadata)
    assert isinstance(operation, CreateSchemaOperation)

    # Test getting drop operation
    operation = schema_state_change.get_operation(mock_schema, None, metadata)
    assert isinstance(operation, DropSchemaOperation)
