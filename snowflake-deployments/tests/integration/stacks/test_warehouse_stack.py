import pytest
import yaml
from aws_cdk import App
from stacks.snowflake.warehouses import WarehouseStack


def test_basic_warehouse_creation(snow):
    """Test creation of a single warehouse with basic configuration"""
    app = App()
    stack = WarehouseStack(app, "TestWarehouses", snow=snow)
    stack.deploy()

    # Verify warehouse was created
    warehouse = stack.warehouses['DEV_ANALYTICS']
    assert warehouse['name'] == 'DEV_ANALYTICS'
    assert warehouse['size'] == 'SMALL'
    assert warehouse['auto_suspend'] == 300
    assert warehouse['environment'] == 'dev'


def test_warehouse_defaults(snow):
    """Test warehouse creation with default values"""
    config = {
        'environments': {
            'dev': {
                'warehouses': [{
                    'name': 'MINIMAL'
                }]
            }
        }
    }

    app = App()
    stack = WarehouseStack(app, "TestWarehouses", snow=snow)
    stack.deploy()

    warehouse = stack.warehouses['DEV_MINIMAL']
    assert warehouse['warehouse_size'] == 'XSMALL'  # default size
    assert warehouse['auto_suspend'] == 60  # default auto-suspend
    assert warehouse['auto_resume'] is True
    assert warehouse['initially_suspended'] is True
    assert warehouse['min_cluster_count'] == 1
    assert warehouse['max_cluster_count'] == 1
    assert warehouse['scaling_policy'] == 'STANDARD'


def test_warehouse_update(snow, basic_config):
    """Test updating existing warehouse configuration"""
    app = App()
    stack = WarehouseStack(app, "TestWarehouses",
                           snow=snow, admin_config=basic_config)

    # Initial deployment
    stack.deploy()

    # Modify configuration
    stack.admin_config['environments']['dev']['warehouses'][0]['size'] = 'MEDIUM'
    stack.admin_config['environments']['dev']['warehouses'][0]['auto_suspend'] = 600

    # Re-deploy with new configuration
    stack.deploy()

    # Verify updates
    warehouse = stack.warehouses['DEV_ANALYTICS']
    assert warehouse['size'] == 'MEDIUM'
    assert warehouse['auto_suspend'] == 600


def test_invalid_warehouse_size(snow):
    """Test handling of invalid warehouse size"""
    config = {
        'environments': {
            'dev': {
                'warehouses': [{
                    'name': 'INVALID',
                    'size': 'INVALID_SIZE'
                }]
            }
        }
    }

    app = App()
    stack = WarehouseStack(app, "TestWarehouses",
                           snow=snow, admin_config=config)

    with pytest.raises(Exception):
        stack.deploy()


def test_warehouse_name_collision(snow):
    """Test handling of duplicate warehouse names"""
    config = {
        'environments': {
            'dev': {
                'warehouses': [
                    {'name': 'ANALYTICS'},
                    {'name': 'ANALYTICS'}  # Duplicate name
                ]
            }
        }
    }

    app = App()
    stack = WarehouseStack(app, "TestWarehouses",
                           snow=snow, admin_config=config)

    with pytest.raises(Exception):
        stack.deploy()
