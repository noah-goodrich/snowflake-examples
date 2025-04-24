"""Tests for warehouse resource."""

import pytest
from unittest.mock import MagicMock, patch

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.warehouse import Warehouse as SnowflakeWarehouse
from resources.warehouse import Warehouse, WarehouseConfig


@patch('snowflake.core.Root')
def test_warehouse_create(mock_root):
    """Test warehouse creation"""
    # Setup mock warehouse
    mock_wh = MagicMock(spec=SnowflakeWarehouse)
    mock_wh.name = "DEV_TEST_WH"
    mock_wh.warehouse_size = "XSMALL"

    # Setup mock warehouses collection
    mock_whs = MagicMock()
    mock_whs.create.return_value = mock_wh

    # Setup mock root
    mock_root.warehouses = mock_whs

    # Create warehouse
    config = WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=1,
        initially_suspended=True,
        scaling_policy="STANDARD"
    )
    warehouse = Warehouse(mock_root, "DEV", config)
    wh = warehouse.create()
    assert wh.warehouse_size == "XSMALL"
    mock_whs.create.assert_called_once()


@patch('snowflake.core.Root')
def test_warehouse_alter(mock_root):
    """Test warehouse alteration"""
    # Setup mock warehouse
    mock_wh = MagicMock(spec=SnowflakeWarehouse)
    mock_wh.name = "DEV_TEST_WH"
    mock_wh.warehouse_size = "XSMALL"

    # Setup mock warehouses collection
    mock_whs = MagicMock()
    mock_whs.create.return_value = mock_wh

    # Setup mock root
    mock_root.warehouses = mock_whs

    # Create initial warehouse
    config = WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=1,
        initially_suspended=True,
        scaling_policy="STANDARD"
    )
    warehouse = Warehouse(mock_root, "DEV", config)
    wh = warehouse.create()
    assert wh.warehouse_size == "XSMALL"

    # Alter warehouse
    new_config = WarehouseConfig(
        name="TEST_WH",
        size="SMALL",
        auto_suspend=120,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=1,
        initially_suspended=True,
        scaling_policy="STANDARD"
    )
    mock_wh.warehouse_size = "SMALL"
    altered_wh = warehouse.alter(new_config)
    assert altered_wh.warehouse_size == "SMALL"
    mock_whs.create.assert_called()


@patch('snowflake.core.Root')
def test_warehouse_drop(mock_root):
    """Test warehouse drop operation"""
    # Setup mock warehouse
    mock_wh = MagicMock(spec=SnowflakeWarehouse)
    mock_wh.name = "DEV_TEST_WH"
    mock_wh.drop = MagicMock()

    # Setup mock warehouses collection
    mock_whs = MagicMock()
    mock_whs.__getitem__.return_value = mock_wh

    # Setup mock root
    mock_root.warehouses = mock_whs

    # Create warehouse
    config = WarehouseConfig(name="TEST_WH")
    warehouse = Warehouse(mock_root, "DEV", config)

    # Drop warehouse
    warehouse.drop(cascade=True)
    mock_wh.drop.assert_called_once_with(cascade=True)


@patch('snowflake.core.Root')
def test_warehouse_get(mock_root):
    """Test warehouse retrieval"""
    # Setup mock warehouse
    mock_wh = MagicMock(spec=SnowflakeWarehouse)
    mock_wh.name = "DEV_TEST_WH"

    # Setup mock warehouses collection
    mock_whs = MagicMock()
    mock_whs.__getitem__.return_value = mock_wh

    # Setup mock root
    mock_root.warehouses = mock_whs

    # Test get existing warehouse
    config = WarehouseConfig(name="TEST_WH")
    warehouse = Warehouse(mock_root, "DEV", config)
    wh = warehouse.get()
    assert wh == mock_wh
    mock_whs.__getitem__.assert_called_once_with("DEV_TEST_WH")


@patch('snowflake.core.Root')
def test_warehouse_exists(mock_root):
    """Test warehouse existence check"""
    # Setup mock warehouses collection
    mock_whs = MagicMock()
    mock_whs.__getitem__.return_value = MagicMock()

    # Setup mock root
    mock_root.warehouses = mock_whs

    # Test existing warehouse
    config = WarehouseConfig(name="TEST_WH")
    warehouse = Warehouse(mock_root, "DEV", config)
    assert warehouse.exists() is True
    mock_whs.__getitem__.assert_called_once_with("DEV_TEST_WH")


@patch('snowflake.core.Root')
def test_warehouse_suspend_resume(mock_root):
    """Test warehouse suspend and resume operations"""
    # Setup mock warehouse
    mock_wh = MagicMock(spec=SnowflakeWarehouse)
    mock_wh.name = "DEV_TEST_WH"

    # Setup mock warehouses collection
    mock_whs = MagicMock()
    mock_whs.__getitem__.return_value = mock_wh

    # Setup mock root and session
    mock_session = MagicMock()
    mock_root.session = mock_session
    mock_root.warehouses = mock_whs

    # Create warehouse
    config = WarehouseConfig(name="TEST_WH")
    warehouse = Warehouse(mock_root, "DEV", config)

    # Test suspend
    warehouse.suspend()
    mock_session.sql.assert_called_with(
        "ALTER WAREHOUSE DEV_TEST_WH SUSPEND")

    # Test resume
    warehouse.resume()
    mock_session.sql.assert_called_with(
        "ALTER WAREHOUSE DEV_TEST_WH RESUME")


def test_warehouse_config_validation():
    """Test warehouse configuration validation"""
    # Test valid config
    config = WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        min_cluster_count=1,
        max_cluster_count=1
    )
    config.validate()  # Should not raise

    # Test invalid size
    with pytest.raises(ValueError, match="Invalid warehouse size"):
        WarehouseConfig(
            name="TEST_WH",
            size="INVALID",
            auto_suspend=60,
            min_cluster_count=1,
            max_cluster_count=1
        ).validate()

    # Test invalid auto_suspend
    with pytest.raises(ValueError, match="auto_suspend must be >= 0"):
        WarehouseConfig(
            name="TEST_WH",
            size="XSMALL",
            auto_suspend=-1,
            min_cluster_count=1,
            max_cluster_count=1
        ).validate()

    # Test invalid cluster count
    with pytest.raises(ValueError, match="min_cluster_count must be >= 1"):
        WarehouseConfig(
            name="TEST_WH",
            size="XSMALL",
            auto_suspend=60,
            min_cluster_count=0,
            max_cluster_count=1
        ).validate()

    # Test invalid max_cluster_count
    with pytest.raises(ValueError, match="max_cluster_count must be >= min_cluster_count"):
        WarehouseConfig(
            name="TEST_WH",
            size="XSMALL",
            auto_suspend=60,
            min_cluster_count=2,
            max_cluster_count=1
        ).validate()


@patch('snowflake.core.Root')
def test_warehouse_name_formatting(mock_root):
    """Test warehouse name formatting with environment prefix"""
    # Test with environment prefix
    config = WarehouseConfig(
        name="TEST_WH",
        prefix_with_environment=True
    )
    warehouse = Warehouse(mock_root, "DEV", config)
    assert warehouse._name == "DEV_TEST_WH"

    # Test without environment prefix
    config = WarehouseConfig(
        name="TEST_WH",
        prefix_with_environment=False
    )
    warehouse = Warehouse(mock_root, "DEV", config)
    assert warehouse._name == "TEST_WH"

    # Test lowercase conversion
    config = WarehouseConfig(
        name="test_wh",
        prefix_with_environment=True
    )
    warehouse = Warehouse(mock_root, "DEV", config)
    assert warehouse._name == "DEV_TEST_WH"

    # Test mixed case handling
    config = WarehouseConfig(
        name="Test_Wh",
        prefix_with_environment=True
    )
    warehouse = Warehouse(mock_root, "DEV", config)
    assert warehouse._name == "DEV_TEST_WH"
