import pytest
from ....forts.medallion import MedallionFort


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
                medallion_fort.snow.warehouses[f"{env_db}_{size}"].drop(
                    True)
            medallion_fort.snow.databases[env_db].drop(True)
            medallion_fort.snow.users[f'{env_db}_owner'].drop(True)
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
                    medallion_fort.snow.warehouses[f"{env_db}_{size}"].drop(
                        True)
                medallion_fort.snow.databases[env_db].drop(True)
            except Exception as e:
                pass


def test_database_creation(medallion_fort: MedallionFort):
    """Test creation of all medallion databases"""
    for db in medallion_fort.snow.databases.iter():
        if db.name in ['COSMERE', 'SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA']:
            continue

        assert db.name in ['DEV_BRONZE',
                           'DEV_SILVER',
                           'DEV_GOLD',
                           'DEV_PLATINUM']


def test_warehouse_creation(medallion_fort: MedallionFort):
    """Test creation of warehouses for each database"""
    for db in medallion_fort.snow.databases.iter():
        if db.name in ['COSMERE', 'SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA']:
            continue

        # Verify warehouses of each size exist
        for size in medallion_fort.WAREHOUSE_SIZES:
            wh = medallion_fort.snow.warehouses[f"{db.name}_{size}"] \
                .fetch()

            assert wh is not None
            # assert wh.warehouse_size.lower().replace('-', '') == size.lower()
            # assert wh.auto_suspend == res.auto_suspend
            # assert wh.min_cluster_count == res.min_cluster_count
            # assert wh.max_cluster_count == res.max_cluster_count


def test_platinum_warehouse_optimization(medallion_fort):
    """Test that PLATINUM warehouses are properly configured"""
    env_db = f"{medallion_fort.env}_PLATINUM"

    # Test standard warehouses
    for size in ['XSmall', 'Small', 'Medium', 'Large']:
        wh = medallion_fort.snow.warehouses[f"{env_db}_{size}"].fetch()
        assert wh is not None
        # assert wh.warehouse_size.lower() == size.lower()

    # Test Snowpark-optimized warehouses
    for size in ['XLARGE', 'XXLARGE', 'XXXLARGE']:
        wh = medallion_fort.snow.warehouses[f"{env_db}_{size}"].fetch()
        assert wh is not None
        # assert wh.warehouse_size.lower().replace('-', '') == size.lower()
        assert wh.enable_query_acceleration == 'true'
        assert wh.query_acceleration_max_scale_factor == 8
