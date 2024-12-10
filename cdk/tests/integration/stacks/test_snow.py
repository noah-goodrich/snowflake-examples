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
    db = stack.snow.databases["DEV_TEST2_DB"].fetch()
    assert db is not None
    assert db.name == "DEV_TEST2_DB"
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
    custom_config = {
        "SMALL": {
            "auto_suspend": 300,
            "min_cluster_count": 2,
            "max_cluster_count": 3,
            "prefix_with_environment": False
        }
    }
    stack.create_or_alter_warehouse("TEST_DB", custom_config)

    wh_name = "test_db_small".upper()
    wh = stack.snow.warehouses[wh_name].fetch()
    assert wh is not None
    assert wh.auto_suspend == 300
    assert wh.min_cluster_count == 2
    assert wh.max_cluster_count == 3


@pytest.mark.skip("Role creation not implemented yet")
def test_role_creation(stack: SnowStack):
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
def test_user_creation(stack: SnowStack):
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


def test_full_deployment(stack: SnowStack):
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
    except:
        pass

    try:
        stack.snow.warehouses["prod_test_db_small"].drop(True)
    except:
        pass

    stack.snow.databases["DEV_TEST_DB"].drop(True)


def test_create_or_alter_access_role(stack: SnowStack):
    """Test creation of database-specific access roles"""

    # Create read-only access role
    stack.create_or_alter_access_role(
        database="TEST_DB",
        level="RO"
    )

    # Create read-write access role that inherits from RO
    stack.create_or_alter_access_role(
        database="TEST_DB",
        level="RW",
        grants_to=["RO"]
    )

    # Verify roles exist and have correct properties
    ro_role = stack.snow.databases["DEV_TEST_DB"] \
        .roles["DEV_TEST_DB_RO"] \
        .fetch()
    assert ro_role is not None
    assert ro_role.name == "DEV_TEST_DB_RO"

    # Verify RO role has correct permissions
    ro_grants = ro_role.show_grants()
    assert "USAGE" in ro_grants
    assert "SELECT" in ro_grants

    # Verify RW role and inheritance
    rw_role = stack.snow.databases["DEV_TEST_DB"] \
        .roles["DEV_TEST_DB_RW"] \
        .fetch()
    assert rw_role is not None
    assert rw_role.name == "DEV_TEST_DB_RW"

    # Verify RW role has correct permissions and inheritance
    rw_grants = rw_role.show_grants()
    assert "INSERT" in rw_grants
    assert "UPDATE" in rw_grants
    assert "DELETE" in rw_grants

    inherited_roles = rw_role.show_grants_to_roles()
    assert "DEV_TEST_DB_RO" in inherited_roles


def test_create_or_alter_access_role_invalid_level(stack: SnowStack):
    """Test that invalid access levels raise appropriate errors"""
    with pytest.raises(ValueError) as exc_info:
        stack.create_or_alter_access_role(
            database="DEV_TEST_DB",
            level="INVALID_LEVEL"
        )
    assert "Invalid access level" in str(exc_info.value)
