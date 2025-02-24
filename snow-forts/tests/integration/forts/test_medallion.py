import pytest
from forts.medallion import MedallionFort
from resources.warehouse import WarehouseConfig
from resources.database import DatabaseConfig


@pytest.fixture(scope="module")
def medallion_fort(snow) -> MedallionFort:
    """Create a fresh Medallion stack instance for each test"""
    stack = MedallionFort(snow=snow, environment="dev")
    return stack


@pytest.fixture(scope='module', autouse=True)
def cleanup(medallion_fort):
    """Cleanup resources before and after each test"""
    databases = ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']

    # Clean up existing resources
    for db_name in databases:
        try:
            env_db = f"{medallion_fort.env}_{db_name}"
            for size in medallion_fort.WAREHOUSE_SIZES:
                medallion_fort.warehouse_manager.drop(f"{env_db}_{size}")
            medallion_fort.database_manager.drop(env_db, cascade=True)
        except Exception as e:
            print(e)

    # Deploy fresh resources
    medallion_fort.deploy()

    try:
        yield
    finally:
        # Cleanup after tests
        for db_name in databases:
            try:
                env_db = f"{medallion_fort.env}_{db_name}"
                for size in medallion_fort.WAREHOUSE_SIZES:
                    medallion_fort.warehouse_manager.drop(f"{env_db}_{size}")
                medallion_fort.database_manager.drop(env_db, cascade=True)
            except Exception as e:
                pass


def test_database_creation(medallion_fort: MedallionFort):
    """Test creation of all medallion databases"""
    databases = medallion_fort.snow.session.sql("SHOW DATABASES").collect()
    db_names = [row['name'] for row in databases]

    expected_dbs = ['DEV_BRONZE', 'DEV_SILVER', 'DEV_GOLD', 'DEV_PLATINUM']
    for db_name in expected_dbs:
        assert db_name in db_names


def test_warehouse_creation(medallion_fort: MedallionFort):
    """Test creation of warehouses for each database"""
    databases = ['DEV_BRONZE', 'DEV_SILVER', 'DEV_GOLD', 'DEV_PLATINUM']

    for db_name in databases:
        # Verify warehouses of each size exist
        for size in medallion_fort.WAREHOUSE_SIZES:
            warehouse = medallion_fort.warehouse_manager.get(
                f"{db_name}_{size}")
            assert warehouse is not None
            assert warehouse.warehouse_size.lower().replace('-', '') == size.lower()


def test_platinum_warehouse_optimization(medallion_fort):
    """Test that PLATINUM warehouses are properly configured"""
    env_db = f"{medallion_fort.env}_PLATINUM"

    # Test standard warehouses
    for size in ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE']:
        warehouse = medallion_fort.warehouse_manager.get(f"{env_db}_{size}")
        assert warehouse is not None
        assert warehouse.warehouse_size == size

    # Test Snowpark-optimized warehouses
    for size in ['XLARGE', 'XXLARGE', 'XXXLARGE']:
        warehouse = medallion_fort.warehouse_manager.get(f"{env_db}_{size}")
        assert warehouse is not None
        assert warehouse.warehouse_size == size
        assert warehouse.enable_query_acceleration == True
        assert warehouse.query_acceleration_max_scale_factor == 8
