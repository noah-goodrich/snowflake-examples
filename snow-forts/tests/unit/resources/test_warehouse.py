import pytest
from unittest.mock import MagicMock
from resources.warehouse import Warehouse, WarehouseConfig
from snowflake.core._common import CreateMode


def test_warehouse_config_validation():
    """Test warehouse configuration validation"""
    # Test valid configuration
    config = WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=2
    )
    config.validate()  # Should not raise

    # Test invalid size
    with pytest.raises(ValueError, match="Invalid warehouse size"):
        WarehouseConfig(name="TEST_WH", size="INVALID").validate()

    # Test invalid auto_suspend
    with pytest.raises(ValueError, match="auto_suspend must be >= 0"):
        WarehouseConfig(name="TEST_WH", auto_suspend=-1).validate()

    # Test invalid cluster count
    with pytest.raises(ValueError, match="min_cluster_count must be >= 1"):
        WarehouseConfig(name="TEST_WH", min_cluster_count=0).validate()

    with pytest.raises(ValueError, match="max_cluster_count must be >= min_cluster_count"):
        WarehouseConfig(
            name="TEST_WH",
            min_cluster_count=2,
            max_cluster_count=1
        ).validate()


def test_warehouse_name_formatting(snow):
    """Test warehouse name formatting with environment prefix"""
    warehouse = Warehouse(snow, "DEV")

    # Test with environment prefix
    config = WarehouseConfig(name="TEST_WH", prefix_with_environment=True)
    assert warehouse._format_name(config.name, True) == "DEV_TEST_WH"

    # Test without environment prefix
    config = WarehouseConfig(name="TEST_WH", prefix_with_environment=False)
    assert warehouse._format_name(config.name, False) == "TEST_WH"


def test_warehouse_creation(snow):
    """Test warehouse creation with different modes"""
    # Setup mock warehouse
    mock_warehouse = MagicMock()
    mock_warehouse.name = "DEV_TEST_WH"
    mock_warehouse.warehouse_size = "XSMALL"
    mock_warehouse.auto_suspend = 60
    mock_warehouse.auto_resume = "true"
    mock_warehouse.min_cluster_count = 1
    mock_warehouse.max_cluster_count = 1
    mock_warehouse.drop = MagicMock()

    snow.warehouses.create.return_value = mock_warehouse

    warehouse = Warehouse(snow, "DEV")
    config = WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=1
    )

    # Test if_not_exists mode
    wh = warehouse.create(config, mode=CreateMode.if_not_exists)
    assert wh.name == "DEV_TEST_WH"
    assert wh.warehouse_size == "XSMALL"

    # Test or_replace mode
    config.auto_suspend = 30
    mock_warehouse.auto_suspend = 30
    wh = warehouse.create(config, mode=CreateMode.or_replace)
    assert wh.auto_suspend == 30


def test_warehouse_alter(snow):
    """Test warehouse alteration"""
    # Setup mock warehouse
    mock_warehouse = MagicMock()
    mock_warehouse.name = "DEV_TEST_WH"
    mock_warehouse.warehouse_size = "SMALL"
    mock_warehouse.auto_suspend = 30
    mock_warehouse.drop = MagicMock()

    snow.warehouses.create.return_value = mock_warehouse
    snow.warehouses.__getitem__.return_value = mock_warehouse

    warehouse = Warehouse(snow, "DEV")

    # Create initial warehouse
    config = WarehouseConfig(name="TEST_WH", size="XSMALL")
    wh = warehouse.create(config)

    # Alter warehouse
    new_config = WarehouseConfig(
        name="TEST_WH",
        size="SMALL",
        auto_suspend=30
    )
    altered_wh = warehouse.alter("TEST_WH", new_config)
    assert altered_wh.warehouse_size == "SMALL"
    assert altered_wh.auto_suspend == 30


def test_warehouse_drop(snow):
    """Test warehouse drop operation"""
    # Setup mock warehouse
    mock_warehouse = MagicMock()
    mock_warehouse.drop = MagicMock()

    snow.warehouses.__getitem__.return_value = mock_warehouse
    snow.warehouses.create.return_value = mock_warehouse

    warehouse = Warehouse(snow, "DEV")

    # Create warehouse
    config = WarehouseConfig(name="TEST_WH")
    warehouse.create(config)

    # Drop warehouse
    warehouse.drop("TEST_WH")
    mock_warehouse.drop.assert_called_once_with(cascade=True)

    # Test get after drop
    snow.warehouses.__getitem__.side_effect = KeyError()
    assert warehouse.get("TEST_WH") is None


def test_warehouse_suspend_resume(snow):
    """Test warehouse suspend and resume operations"""
    # Setup mock warehouse
    mock_warehouse = MagicMock()
    mock_warehouse.name = "DEV_TEST_WH"
    mock_warehouse.drop = MagicMock()

    snow.warehouses.create.return_value = mock_warehouse
    snow.warehouses.__getitem__.return_value = mock_warehouse

    warehouse = Warehouse(snow, "DEV")

    # Create warehouse
    config = WarehouseConfig(name="TEST_WH", initially_suspended=False)
    wh = warehouse.create(config)

    # Test suspend
    warehouse.suspend("TEST_WH")
    snow.session.sql.assert_called_with(
        "ALTER WAREHOUSE DEV_TEST_WH SUSPEND"
    )

    # Test resume
    warehouse.resume("TEST_WH")
    snow.session.sql.assert_called_with(
        "ALTER WAREHOUSE DEV_TEST_WH RESUME"
    )
