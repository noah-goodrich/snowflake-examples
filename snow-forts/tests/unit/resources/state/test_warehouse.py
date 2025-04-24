"""Tests for warehouse state management."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch

from snowflake.core import Root
from resources.state.warehouse import (
    WarehouseStateChange,
    CreateWarehouseOperation,
    AlterWarehouseOperation,
    DropWarehouseOperation
)
from resources.warehouse import Warehouse, WarehouseConfig
from resources.state.base import StateChangeMetadata, OperationType


@pytest.fixture
def mock_warehouse():
    """Create a mock warehouse."""
    warehouse = MagicMock(spec=Warehouse)
    warehouse.name = "TEST_WH"
    warehouse.warehouse_size = "XSMALL"
    warehouse.auto_suspend = 60
    warehouse.auto_resume = True
    warehouse.min_cluster_count = 1
    warehouse.max_cluster_count = 1
    warehouse.scaling_policy = "STANDARD"
    warehouse.enable_query_acceleration = False
    warehouse.query_acceleration_max_scale_factor = 8
    return warehouse


@pytest.fixture
def warehouse_config():
    """Create a warehouse configuration."""
    return WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=1,
        scaling_policy="STANDARD",
        enable_query_acceleration=False,
        query_acceleration_max_scale_factor=8,
        initially_suspended=True,
        prefix_with_environment=True
    )


def test_warehouse_state_change_check_current_state(mock_warehouse):
    """Test checking current warehouse state."""
    state_change = WarehouseStateChange(MagicMock())
    current_state = state_change.check_current_state(mock_warehouse)

    assert current_state["name"] == "TEST_WH"
    assert current_state["size"] == "XSMALL"
    assert current_state["auto_suspend"] == 60
    assert current_state["auto_resume"] is True
    assert current_state["min_cluster_count"] == 1
    assert current_state["max_cluster_count"] == 1
    assert current_state["scaling_policy"] == "STANDARD"
    assert current_state["enable_query_acceleration"] is False
    assert current_state["query_acceleration_max_scale_factor"] == 8


def test_warehouse_state_change_validate_state():
    """Test warehouse state validation."""
    state_change = WarehouseStateChange(MagicMock())

    # Test valid state
    valid_state = {
        "name": "TEST_WH",
        "size": "XSMALL"
    }
    assert state_change.validate_state(valid_state) is True

    # Test missing required fields
    invalid_state = {
        "name": "TEST_WH"
    }
    assert state_change.validate_state(invalid_state) is False

    # Test invalid size
    invalid_size_state = {
        "name": "TEST_WH",
        "size": "INVALID_SIZE"
    }
    assert state_change.validate_state(invalid_size_state) is False


def test_create_warehouse_operation(mock_warehouse, warehouse_config):
    """Test warehouse creation operation."""
    metadata = StateChangeMetadata("test_id", "test description")
    operation = CreateWarehouseOperation(
        mock_warehouse, warehouse_config, metadata)

    # Test validation when warehouse doesn't exist
    mock_warehouse.exists.return_value = False
    assert operation.validate() is True

    # Test validation when warehouse exists
    mock_warehouse.exists.return_value = True
    assert operation.validate() is False

    # Test apply
    operation.apply()
    mock_warehouse.create.assert_called_once_with(warehouse_config)

    # Test rollback
    operation.rollback()
    mock_warehouse.drop.assert_called_once_with(warehouse_config.name)


def test_alter_warehouse_operation(mock_warehouse, warehouse_config):
    """Test warehouse alteration operation."""
    metadata = StateChangeMetadata("test_id", "test description")
    operation = AlterWarehouseOperation(
        mock_warehouse, warehouse_config, metadata)

    # Test validation when warehouse exists
    mock_warehouse.exists.return_value = True
    assert operation.validate() is True

    # Test validation when warehouse doesn't exist
    mock_warehouse.exists.return_value = False
    assert operation.validate() is False

    # Test apply
    operation.apply()
    mock_warehouse.alter.assert_called_once_with(
        warehouse_config.name, warehouse_config)

    # Test rollback
    operation.rollback()
    mock_warehouse.alter.assert_called_with(
        warehouse_config.name, warehouse_config)


def test_warehouse_state_change_get_operation(mock_warehouse, warehouse_config):
    """Test getting appropriate warehouse operation."""
    state_change = WarehouseStateChange(MagicMock())
    metadata = StateChangeMetadata("test_id", "test description")

    # Test getting create operation
    mock_warehouse.exists.return_value = False
    operation = state_change.get_operation(
        mock_warehouse, warehouse_config, metadata)
    assert isinstance(operation, CreateWarehouseOperation)

    # Test getting alter operation
    mock_warehouse.exists.return_value = True
    operation = state_change.get_operation(
        mock_warehouse, warehouse_config, metadata)
    assert isinstance(operation, AlterWarehouseOperation)
