import pytest
from ....forts.fort import SnowFort
from aws_cdk import App


@pytest.fixture(scope="function")
def fort(snow) -> SnowFort:
    """Create a fresh SnowStack instance for each test"""
    stack = SnowFort(snow=snow, environment="dev")
    return stack


def test_create_or_alter_warehouse_no_overrides(fort: SnowFort):
    """Test warehouse creation with different sizes and environments"""
    # Test basic warehouse creation
    fort.create_or_alter_warehouse("TEST_DB", "XSMALL")

    # Verify warehouse exists with correct properties
    wh_name = "dev_test_db_xsmall"
    wh = fort.snow.warehouses[wh_name].fetch()
    assert wh is not None
    assert wh.name == wh_name.upper()
    assert wh.warehouse_size == "X-Small"


def test_create_or_alter_warehouse_with_overrides(fort: SnowFort):
    # Test warehouse with custom properties
    custom_config = {
        "auto_suspend": 300,
        "min_cluster_count": 2,
        "max_cluster_count": 3,
        "prefix_with_environment": False
    }
    fort.create_or_alter_warehouse(
        "TEST_DB", 'SMALL', overrides=custom_config)

    wh_name = "test_db_small".upper()
    wh = fort.snow.warehouses[wh_name].fetch()
    assert wh is not None
    assert wh.auto_suspend == 300
    assert wh.min_cluster_count == 2
    assert wh.max_cluster_count == 3


def test_database_roles(fort: SnowFort):
    """Test creation of database roles"""
    env_db = "DEV_TEST_DB"
    fort.snow.session.use_database(env_db)
    # Verify roles exist
    roles = list(fort.snow.databases[env_db].database_roles.iter())
    assert len(roles) == 2
    assert any(r.name == "READ_ONLY" for r in roles)
    assert any(r.name == "READ_WRITE" for r in roles)


def test_role_privileges(fort: SnowFort):
    """Test proper privilege assignment for roles"""
    env_db = "DEV_TEST_DB"
    fort.snow.session.use_database(env_db)

    # Get roles
    ro_role = fort.snow.databases[env_db].database_roles["READ_ONLY"]
    rw_role = fort.snow.databases[env_db].database_roles["READ_WRITE"]

    def _verify_role_grants(role, expected_grants, expected_future_grants):
        def _v(grants_to, expected):
            grants = list(grants_to)
            keepers = ['securable', 'securable_type',
                       'grant_option', 'privileges']
            grants_dicts = [{k: g.to_dict()[k] for k in keepers}
                            for g in grants]
            assert len(grants) == len(expected)
            assert any(e in grants_dicts for e in expected)

        _v(role.iter_grants_to(), expected_grants)
        _v(role.iter_future_grants_to(), expected_future_grants)

    # Verify RO privileges
    _verify_role_grants(ro_role, [{
        'securable': {'name': 'DEV_TEST_DB'},
        'securable_type': 'DATABASE',
        'grant_option': False,
        'privileges': ['USAGE']
    }], [{
        'securable': {'database': 'DEV_TEST_DB', 'name': '"<TABLE>"'},
        'securable_type': 'TABLE',
        'grant_option': False,
        'privileges': ['SELECT']
    }])

    # Verify RW privileges
    _verify_role_grants(rw_role, [{
        'securable': {'name': 'DEV_TEST_DB'},
        'securable_type': 'DATABASE',
        'grant_option': False,
        'privileges': ['USAGE']
    }, {
        'securable': {'database': 'DEV_TEST_DB', 'name': 'DEV_TEST_DB_RO'},
        'securable_type': 'DATABASE ROLE',
        'grant_option': False,
        'privileges': ['USAGE']
    }], [{
        'securable': {'database': 'DEV_TEST_DB', 'name': '"<TABLE>"'},
        'securable_type': 'TABLE',
        'grant_option': False,
        'privileges': ['DELETE']
    }, {
        'securable': {'database': 'DEV_TEST_DB', 'name': '"<TABLE>"'},
        'securable_type': 'TABLE',
        'grant_option': False,
        'privileges': ['INSERT']
    }, {
        'securable': {'database': 'DEV_TEST_DB', 'name': '"<TABLE>"'},
        'securable_type': 'TABLE',
        'grant_option': False,
        'privileges': ['SELECT']
    }, {
        'securable': {'database': 'DEV_TEST_DB', 'name': '"<TABLE>"'},
        'securable_type': 'TABLE',
        'grant_option': False,
        'privileges': ['UPDATE']
    }])


@pytest.fixture(autouse=True)
def setup_teardown(fort: SnowFort):
    """Cleanup resources after each test"""
    fort.snow.databases["DEV_TEST_DB"].drop(True)

    # First create a test database
    fort.create_if_not_exists_database(
        "TEST_DB", "Test Database", prefix_with_environment=True)

    yield

    # Clean up any resources created during tests
    try:
        fort.snow.warehouses["dev_test_db_xsmall"].drop(True)
        fort.snow.warehouses["prod_test_db_small"].drop(True)
        fort.snow.databases["DEV_TEST_DB"].drop(True)
        fort.snow.databases["DEV_TEST_DB2"].drop(True)
    except:
        pass
