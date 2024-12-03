import pytest
from aws_cdk import App
from stacks.snowflake.databases import DatabaseStack


@pytest.fixture
def basic_config():
    """Basic database configuration"""
    return {
        'environments': {
            'dev': {
                'databases': [{
                    'name': 'ANALYTICS',
                    'schemas': ['RAW', 'TRANSFORMED'],
                    'comment': 'Analytics database for development'
                }]
            }
        }
    }


@pytest.fixture
def multi_env_config():
    """Configuration with multiple environments"""
    return {
        'environments': {
            'dev': {
                'databases': [{
                    'name': 'ANALYTICS',
                    'schemas': ['RAW']
                }]
            },
            'prod': {
                'databases': [{
                    'name': 'ANALYTICS',
                    'schemas': ['RAW', 'CURATED']
                }]
            }
        }
    }


def test_basic_database_creation(snow, basic_config):
    """Test creation of a single database with basic configuration"""
    app = App()
    stack = DatabaseStack(app, "TestDatabases", snow=snow,
                          admin_config=basic_config)
    stack.deploy()

    # Verify database was created
    database = stack.databases['DEV_ANALYTICS']
    assert database['name'] == 'DEV_ANALYTICS'
    assert 'RAW' in database['schemas']
    assert 'TRANSFORMED' in database['schemas']
    assert database['environment'] == 'dev'


def test_multiple_environments(snow, multi_env_config):
    """Test creation of databases in multiple environments"""
    app = App()
    stack = DatabaseStack(app, "TestDatabases", snow=snow,
                          admin_config=multi_env_config)
    stack.deploy()

    # Verify dev database
    dev_db = stack.databases['DEV_ANALYTICS']
    assert len(dev_db['schemas']) == 1
    assert 'RAW' in dev_db['schemas']

    # Verify prod database
    prod_db = stack.databases['PROD_ANALYTICS']
    assert len(prod_db['schemas']) == 2
    assert 'RAW' in prod_db['schemas']
    assert 'CURATED' in prod_db['schemas']


def test_database_schema_update(snow, basic_config):
    """Test updating existing database schemas"""
    app = App()
    stack = DatabaseStack(app, "TestDatabases", snow=snow,
                          admin_config=basic_config)

    # Initial deployment
    stack.deploy()

    # Modify configuration to add new schema
    stack.admin_config['environments']['dev']['databases'][0]['schemas'].append(
        'REPORTING')

    # Re-deploy with new configuration
    stack.deploy()

    # Verify updates
    database = stack.databases['DEV_ANALYTICS']
    assert 'REPORTING' in database['schemas']


def test_invalid_database_name(snow):
    """Test handling of invalid database name"""
    config = {
        'environments': {
            'dev': {
                'databases': [{
                    'name': 'INVALID-NAME',  # Invalid character in name
                    'schemas': ['RAW']
                }]
            }
        }
    }

    app = App()
    stack = DatabaseStack(app, "TestDatabases", snow=snow, admin_config=config)

    with pytest.raises(Exception):
        stack.deploy()


def test_database_name_collision(snow):
    """Test handling of duplicate database names"""
    config = {
        'environments': {
            'dev': {
                'databases': [
                    {'name': 'ANALYTICS', 'schemas': ['RAW']},
                    {'name': 'ANALYTICS', 'schemas': [
                        'TRANSFORMED']}  # Duplicate name
                ]
            }
        }
    }

    app = App()
    stack = DatabaseStack(app, "TestDatabases", snow=snow, admin_config=config)

    with pytest.raises(Exception):
        stack.deploy()


def test_empty_schema_list(snow):
    """Test handling of database with no schemas"""
    config = {
        'environments': {
            'dev': {
                'databases': [{
                    'name': 'ANALYTICS',
                    'schemas': []
                }]
            }
        }
    }

    app = App()
    stack = DatabaseStack(app, "TestDatabases", snow=snow, admin_config=config)
    stack.deploy()

    # Verify database was created even with empty schema list
    database = stack.databases['DEV_ANALYTICS']
    assert database['name'] == 'DEV_ANALYTICS'
    assert len(database['schemas']) == 0
