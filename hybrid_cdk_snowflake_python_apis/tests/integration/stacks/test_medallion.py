import pytest
from aws_cdk import App
from ....stacks.medallion import Medallion


@pytest.fixture(scope='module')
def medallion_stack(snow) -> Medallion:
    app = App()
    stack = Medallion(app, "TestMedallion", snow)
    return stack


def test_create_databases(medallion_stack: Medallion):
    assert medallion_stack.snow.databases['DEV_BRONZE'] is not None
    assert medallion_stack.snow.databases['DEV_SILVER'] is not None
    assert medallion_stack.snow.databases['DEV_GOLD'] is not None
    assert medallion_stack.snow.databases['DEV_PLATINUM'] is not None


def test_deploy_creates_warehouses(medallion_stack: Medallion):
    assert medallion_stack.snow.warehouses['DEV_BRONZE_XS'] is not None
    assert medallion_stack.snow.warehouses['DEV_BRONZE_S'] is not None
    assert medallion_stack.snow.warehouses['DEV_BRONZE_M'] is not None
    assert medallion_stack.snow.warehouses['DEV_BRONZE_L'] is not None
    assert medallion_stack.snow.warehouses['DEV_BRONZE_XL'] is not None
    assert medallion_stack.snow.warehouses['DEV_BRONZE_2XL'] is not None
    assert medallion_stack.snow.warehouses['DEV_BRONZE_3XL'] is not None

    assert medallion_stack.snow.warehouses['DEV_SILVER_XS'] is not None
    assert medallion_stack.snow.warehouses['DEV_SILVER_S'] is not None
    assert medallion_stack.snow.warehouses['DEV_SILVER_M'] is not None
    assert medallion_stack.snow.warehouses['DEV_SILVER_L'] is not None
    assert medallion_stack.snow.warehouses['DEV_SILVER_XL'] is not None
    assert medallion_stack.snow.warehouses['DEV_SILVER_2XL'] is not None
    assert medallion_stack.snow.warehouses['DEV_SILVER_3XL'] is not None


@pytest.fixture(scope='module', autouse=True)
def setup_teardown(medallion_stack: Medallion):
    medallion_stack.snow.databases['DEV_BRONZE'].drop(True)
    medallion_stack.snow.databases['DEV_SILVER'].drop(True)
    medallion_stack.snow.databases['DEV_GOLD'].drop(True)
    medallion_stack.snow.databases['DEV_PLATINUM'].drop(True)

    medallion_stack.deploy()

    yield

    medallion_stack.snow.databases['DEV_BRONZE'].drop(True)
    medallion_stack.snow.databases['DEV_SILVER'].drop(True)
    medallion_stack.snow.databases['DEV_GOLD'].drop(True)
    medallion_stack.snow.databases['DEV_PLATINUM'].drop(True)
