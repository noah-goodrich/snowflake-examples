import pytest
from forts.fort import SnowFort
from resources.warehouse import WarehouseConfig
from resources.database import DatabaseConfig
from resources.role import RoleConfig


@pytest.fixture(scope="function")
def fort(snow) -> SnowFort:
    """Create a fresh SnowStack instance for each test"""
    return SnowFort(snow=snow, environment="dev")


@pytest.fixture(autouse=True)
def setup_teardown(fort: SnowFort):
    """Cleanup resources after each test"""
    try:
        fort.database_manager.drop("TEST_DB", cascade=True)
        fort.warehouse_manager.drop("TEST_WH")
    except Exception as e:
        print(f"Setup cleanup error: {e}")

    yield

    try:
        fort.database_manager.drop("TEST_DB", cascade=True)
        fort.warehouse_manager.drop("TEST_WH")
    except Exception as e:
        print(f"Teardown cleanup error: {e}")


def test_warehouse_creation(fort: SnowFort):
    """Test warehouse creation with default configuration"""
    # Create warehouse
    warehouse = fort.warehouse_manager.create(WarehouseConfig(
        name="TEST_WH",
        size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        min_cluster_count=1,
        max_cluster_count=1,
        prefix_with_environment=True
    ))

    assert warehouse is not None
    assert warehouse.name == "DEV_TEST_WH"
    assert warehouse.warehouse_size == "XSMALL"
    assert warehouse.auto_suspend == 60


def test_database_creation(fort: SnowFort):
    """Test database creation with schemas"""
    # Create database
    database = fort.database_manager.create(DatabaseConfig(
        name="TEST_DB",
        schemas=["SCHEMA1", "SCHEMA2"],
        comment="Test database",
        prefix_with_environment=True
    ))

    assert database is not None
    assert database.name == "DEV_TEST_DB"

    # Verify schemas
    schemas = [schema.name for schema in database.schemas]
    assert "SCHEMA1" in schemas
    assert "SCHEMA2" in schemas


def test_database_roles(fort: SnowFort):
    """Test database role creation and privileges"""
    # Create database
    fort.database_manager.create(DatabaseConfig(
        name="TEST_DB",
        prefix_with_environment=True
    ))

    # Create roles
    admin_role = fort.role_manager.create(RoleConfig(
        name="TEST_DB_ADMIN",
        comment="Admin role for TEST_DB",
        prefix_with_environment=True
    ))
    write_role = fort.role_manager.create(RoleConfig(
        name="TEST_DB_WRITE",
        comment="Write role for TEST_DB",
        prefix_with_environment=True
    ))
    read_role = fort.role_manager.create(RoleConfig(
        name="TEST_DB_READ",
        comment="Read role for TEST_DB",
        prefix_with_environment=True
    ))

    assert admin_role.name == "DEV_TEST_DB_ADMIN"
    assert write_role.name == "DEV_TEST_DB_WRITE"
    assert read_role.name == "DEV_TEST_DB_READ"

    # Grant privileges
    fort.role_manager.grant_privilege(
        "TEST_DB_ADMIN",
        "OWNERSHIP",
        "DATABASE",
        "DEV_TEST_DB"
    )
    fort.role_manager.grant_privilege(
        "TEST_DB_WRITE",
        "WRITE",
        "DATABASE",
        "DEV_TEST_DB"
    )
    fort.role_manager.grant_privilege(
        "TEST_DB_READ",
        "READ",
        "DATABASE",
        "DEV_TEST_DB"
    )
