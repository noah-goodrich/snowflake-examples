import pytest
from ....stacks.snow import SnowStack
from aws_cdk import App
from snowflake.core.database import Database
from snowflake.core.warehouse import Warehouse


@pytest.fixture(scope="function")
def stack(snow) -> SnowStack:
    """Create a fresh SnowStack instance for each test"""
    app = App()
    stack = SnowStack(app, "TestStack", snow, environment="dev")
    return stack


def test_create_if_not_exists_database(stack: SnowStack):
    """Test database creation with basic configuration"""
    # Create database
    stack.create_if_not_exists_database(
        "TEST_DB2", "Test Database", prefix_with_environment=True)

    # Verify database exists and has correct properties
    db = stack.snow.databases["DEV_TEST_DB2"].fetch()
    assert db is not None
    assert db.name == "DEV_TEST_DB2"
    assert db.comment == "Test Database"


def test_create_or_alter_warehouse_no_overrides(stack: SnowStack):
    """Test warehouse creation with different sizes and environments"""
    # Test basic warehouse creation
    stack.create_or_alter_warehouse("TEST_DB", "XSMALL")

    # Verify warehouse exists with correct properties
    wh_name = "dev_test_db_xsmall"
    wh = stack.snow.warehouses[wh_name].fetch()
    assert wh is not None
    assert wh.name == wh_name.upper()
    assert wh.warehouse_size == "X-Small"


def test_create_or_alter_warehouse_with_overrides(stack: SnowStack):
    # Test warehouse with custom properties
    stack.create_or_alter_warehouse("TEST_DB", 'SMALL',  {
        "auto_suspend": 300,
        "min_cluster_count": 2,
        "max_cluster_count": 3,
        "prefix_with_environment": False
    })

    wh_name = "test_db_small".upper()
    wh = stack.snow.warehouses[wh_name].fetch()
    assert wh is not None
    assert wh.auto_suspend == 300
    assert wh.min_cluster_count == 2
    assert wh.max_cluster_count == 3


@pytest.fixture(autouse=True)
def setup_teardown(stack: SnowStack):
    """Cleanup resources after each test"""
    stack.snow.databases["DEV_TEST_DB"].drop(True)

    # First create a test database
    stack.create_if_not_exists_database(
        "TEST_DB", "Test Database", prefix_with_environment=True)

    yield

    # Clean up any resources created during tests
    try:
        stack.snow.warehouses["dev_test_db_xsmall"].drop(True)
        stack.snow.warehouses["prod_test_db_small"].drop(True)
        stack.snow.databases["DEV_TEST_DB"].drop(True)
        stack.snow.databases["DEV_TEST_DB2"].drop(True)
    except:
        pass
