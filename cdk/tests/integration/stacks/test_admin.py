import pytest
from aws_cdk import App
from ....stacks.admin import Admin
from snowflake.core.warehouse import Warehouse
from snowflake.core.database import Database
from snowflake.core.role import Role
from snowflake.core.user import User


@pytest.fixture(scope="function")
def admin_stack(snow) -> Admin:
    """Create a fresh Admin stack instance for each test"""
    app = App()
    stack = Admin(app, "TestAdmin", snow)
    return stack


def test_admin_warehouse_creation(admin_stack):
    """Test admin warehouse creation with configuration"""
    admin_stack.deploy()

    # Verify warehouse exists with correct properties
    wh = admin_stack.snow.warehouses["ADMIN_XSMALL"].fetch()
    assert wh is not None
    assert wh.name == "ADMIN_XSMALL"
    assert wh.warehouse_size == "X-Small"
    assert wh.auto_suspend == 60
    assert wh.auto_resume is True
    assert wh.initially_suspended is True


def test_cosmere_database_creation(admin_stack):
    """Test COSMERE database creation with schemas"""
    admin_stack.deploy()

    # Verify database exists
    db = admin_stack.snow.databases["COSMERE"].fetch()
    assert db is not None
    assert db.name == "COSMERE"
    assert db.comment == "Administrative database for platform management"

    # Verify schemas exist
    schemas = admin_stack.snow.databases["COSMERE"].schemas.list()
    schema_names = [schema.name for schema in schemas]
    assert "ADMIN" in schema_names
    assert "AUDIT" in schema_names
    assert "SECURITY" in schema_names


def test_hoid_role_creation(admin_stack):
    """Test HOID administrative role creation"""
    admin_stack.deploy()

    # Verify role exists
    role = admin_stack.snow.roles["HOID"].fetch()
    assert role is not None
    assert role.name == "HOID"
    assert role.comment == "Administrative role for platform automation"

    # Verify role has been granted system roles
    grants = admin_stack.snow.security.grants.list_grants_to_role("HOID")
    granted_roles = [grant.role for grant in grants]
    assert "SECURITYADMIN" in granted_roles
    assert "SYSADMIN" in granted_roles


def test_svc_hoid_user_creation(admin_stack):
    """Test service account creation and configuration"""
    admin_stack.deploy()

    # Verify user exists
    user = admin_stack.snow.users["SVC_HOID"].fetch()
    assert user is not None
    assert user.name == "SVC_HOID"
    assert user.comment == "Service account for administrative automation"
    assert user.default_role == "HOID"
    assert user.default_warehouse == "ADMIN_XSMALL"

    # Verify role grant
    grants = admin_stack.snow.security.grants.list_grants_to_user("SVC_HOID")
    assert "HOID" in [grant.role for grant in grants]


@pytest.fixture(autouse=True)
def cleanup(admin_stack):
    """Cleanup resources after each test"""
    yield

    try:
        # Drop resources in reverse order of creation
        admin_stack.snow.users["SVC_HOID"].drop()
        admin_stack.snow.roles["HOID"].drop()
        admin_stack.snow.databases["COSMERE"].drop(cascade=True)
        admin_stack.snow.warehouses["ADMIN_XSMALL"].drop()
    except:
        pass
