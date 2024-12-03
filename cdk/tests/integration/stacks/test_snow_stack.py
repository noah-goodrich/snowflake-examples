import pytest
from ....stacks.snow_stack import SnowStack
from aws_cdk import App
from snowflake.core.database import Database
from snowflake.core.warehouse import Warehouse


@pytest.fixture(scope="function")
def stack(snow) -> SnowStack:
    """Create a fresh SnowStack instance for each test"""
    app = App()
    stack = SnowStack(app, "TestStack", snow)
    return stack


def test_test(snow):
    snow.databases["TEST_DB"].create_or_alter(Database(
        name="TEST_DB",
        comment="Test Database"
    ))


def test_database_creation(stack):
    """Test database creation with basic configuration"""
    test_config = {
        "description": "Test Database",
        "prefix_with_environment": True
    }

    # Create database
    stack.database("TEST_DB", test_config)

    # Verify database exists and has correct properties
    db = stack.snow.databases["TEST_DB"].describe()
    assert db is not None
    assert db.name == "TEST_DB"
    assert db.comment == "Test Database"


def test_warehouse_creation(stack):
    """Test warehouse creation with different sizes and environments"""
    # Test basic warehouse creation
    stack.warehouse("TEST_DB", "XSMALL", "DEV")

    # Verify warehouse exists with correct properties
    wh_name = "dev_test_db_xsmall"
    wh = stack.snow.warehouses[wh_name].describe()
    assert wh is not None
    assert wh.name == wh_name
    assert wh.warehouse_size == "XSMALL"

    # Test warehouse with custom properties
    custom_config = {
        "SMALL": {
            "auto_suspend": 300,
            "min_cluster_count": 2,
            "max_cluster_count": 3
        }
    }
    stack.warehouse("TEST_DB", custom_config, "PROD")

    wh_name = "prod_test_db_small"
    wh = stack.snow.warehouses[wh_name].describe()
    assert wh is not None
    assert wh.auto_suspend == 300
    assert wh.min_cluster_count == 2
    assert wh.max_cluster_count == 3


@pytest.mark.skip("Role creation not implemented yet")
def test_role_creation(stack):
    """Test role creation with basic configuration"""
    test_config = {
        "warehouses": ["TEST_WAREHOUSE"],
        "databases": {
            "TEST_DB": ["USAGE"]
        }
    }

    stack.role("TEST_ROLE", test_config)

    # Verify role exists
    role = stack.snow.roles["TEST_ROLE"].describe()
    assert role is not None
    assert role.name == "TEST_ROLE"


@pytest.mark.skip("User creation not implemented yet")
def test_user_creation(stack):
    """Test user creation with basic configuration"""
    test_config = {
        "password": "TestPassword123!",
        "roles": ["TEST_ROLE"],
        "default_warehouse": "TEST_WAREHOUSE",
        "default_role": "TEST_ROLE"
    }

    stack.user("TEST_USER", test_config)

    # Verify user exists with correct properties
    user = stack.snow.users["TEST_USER"].describe()
    assert user is not None
    assert user.name == "TEST_USER"
    assert user.default_role == "TEST_ROLE"
    assert user.default_warehouse == "TEST_WAREHOUSE"


def test_full_deployment(stack):
    """Test deploying multiple resources together"""
    # Create database
    db_config = {
        "description": "Test Database",
        "prefix_with_environment": True
    }
    stack.database("TEST_DB", db_config)

    # Create warehouse
    stack.warehouse("TEST_DB", "XSMALL", "DEV")

    # Verify all resources exist
    assert stack.snow.databases["TEST_DB"].describe() is not None
    assert stack.snow.warehouses["dev_test_db_xsmall"].describe() is not None


@pytest.fixture(autouse=True)
def cleanup(stack):
    """Cleanup resources after each test"""
    yield

    # Clean up any resources created during tests
    try:
        stack.snow.warehouses["dev_test_db_xsmall"].drop()
    except:
        pass

    try:
        stack.snow.warehouses["prod_test_db_small"].drop()
    except:
        pass

    try:
        stack.snow.databases["TEST_DB"].drop()
    except:
        pass
