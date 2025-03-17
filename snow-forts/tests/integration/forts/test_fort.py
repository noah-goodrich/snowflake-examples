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


def test_create_or_alter_warehouse(fort: SnowFort):
    """Test end-to-end warehouse creation with default configuration"""
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


def test_create_database_with_roles(fort: SnowFort):
    """Test end-to-end database creation with roles"""
    # Create database
    database = fort.database_manager.create(DatabaseConfig(
        name="TEST_DB",
        schemas=["SCHEMA1", "SCHEMA2"],
        comment="Test database",
        prefix_with_environment=True
    ))

    assert database is not None
    assert database.name == "DEV_TEST_DB"

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

    # Get all SQL calls for debugging
    sql_calls = [call[0][0] for call in fort.snow.session.sql.call_args_list]
    print("\nActual SQL calls:")
    for call in sql_calls:
        print(f"  {call}")

    # Verify SQL calls for privilege grants
    expected_grants = [
        "GRANT OWNERSHIP ON DATABASE DEV_TEST_DB TO ROLE DEV_TEST_DB_ADMIN",
        "GRANT WRITE ON DATABASE DEV_TEST_DB TO ROLE DEV_TEST_DB_WRITE",
        "GRANT READ ON DATABASE DEV_TEST_DB TO ROLE DEV_TEST_DB_READ"
    ]

    for expected_grant in expected_grants:
        assert any(
            expected_grant in call for call in sql_calls
        ), f"Grant not found in SQL calls: {expected_grant}"
