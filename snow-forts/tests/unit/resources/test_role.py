import pytest
from unittest.mock import MagicMock, patch
from resources.role import Role, RoleConfig
from snowflake.core._common import CreateMode
from snowflake.core.role import Role as SnowflakeRole, Securable as RoleSecurable


def test_role_config_validation():
    """Test role configuration validation"""
    # Test valid configuration
    config = RoleConfig(
        name="TEST_ROLE",
        comment="Test role",
        granted_roles=["ROLE1", "ROLE2"]
    )
    config.validate()  # Should not raise

    # Test empty name
    with pytest.raises(ValueError, match="Role name cannot be empty"):
        RoleConfig(name="").validate()

    # Test empty granted role name
    with pytest.raises(ValueError, match="Granted role names cannot be empty"):
        RoleConfig(name="TEST_ROLE", granted_roles=["ROLE1", ""]).validate()

    # Test None name
    with pytest.raises(ValueError, match="Role name cannot be empty"):
        RoleConfig(name=None).validate()


def test_role_name_formatting(snow):
    """Test role name formatting with environment prefix"""
    role = Role(snow, "DEV")

    # Test with environment prefix
    config = RoleConfig(name="TEST_ROLE", prefix_with_environment=True)
    assert role._format_name(config.name, True) == "DEV_TEST_ROLE"

    # Test without environment prefix
    config = RoleConfig(name="TEST_ROLE", prefix_with_environment=False)
    assert role._format_name(config.name, False) == "TEST_ROLE"

    # Test lowercase conversion
    config = RoleConfig(name="test_role", prefix_with_environment=True)
    assert role._format_name(config.name, True) == "DEV_TEST_ROLE"

    # Test mixed case handling
    config = RoleConfig(name="Test_Role", prefix_with_environment=True)
    assert role._format_name(config.name, True) == "DEV_TEST_ROLE"


@patch('snowflake.core.Root')
def test_role_creation(mock_root):
    """Test role creation with different modes"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"
    mock_role.comment = "Test role"
    mock_role.grant_role = MagicMock()

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.return_value = mock_role
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")
    config = RoleConfig(
        name="TEST_ROLE",
        comment="Test role",
        granted_roles=["SYSADMIN"]
    )

    # Test if_not_exists mode
    r = role.create(config, mode=CreateMode.if_not_exists)
    assert r.name == "DEV_TEST_ROLE"
    assert r.comment == "Test role"

    # Verify grant_role was called for granted_roles
    mock_role.grant_role.assert_called_once_with(
        role_type='ROLE',
        role=RoleSecurable(name="SYSADMIN")
    )

    # Test or_replace mode
    config.comment = "Updated comment"
    mock_role.comment = "Updated comment"
    r = role.create(config, mode=CreateMode.or_replace)
    assert r.comment == "Updated comment"


@patch('snowflake.core.Root')
def test_role_alter(mock_root):
    """Test role alteration"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"
    mock_role.comment = "Initial comment"
    mock_role.grant_role = MagicMock()

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.return_value = mock_role
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Create initial role
    config = RoleConfig(name="TEST_ROLE", comment="Initial comment")
    r = role.create(config)
    assert r.comment == "Initial comment"

    # Alter role
    new_config = RoleConfig(
        name="TEST_ROLE",
        comment="Updated comment",
        granted_roles=["SYSADMIN"]
    )
    mock_role.comment = "Updated comment"
    altered_r = role.alter("TEST_ROLE", new_config)
    assert altered_r.comment == "Updated comment"

    # Verify grant_role was called
    mock_role.grant_role.assert_called_with(
        role_type='ROLE',
        role=RoleSecurable(name="SYSADMIN")
    )


@patch('snowflake.core.Root')
def test_role_drop(mock_root):
    """Test role drop operation"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"
    mock_role.drop = MagicMock()

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.return_value = mock_role
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Create role
    config = RoleConfig(name="TEST_ROLE")
    r = role.create(config)

    # Drop role
    role.drop("TEST_ROLE", cascade=True)
    mock_role.drop.assert_called_once_with(cascade=True)

    # Test drop of non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    role.drop("NONEXISTENT_ROLE")  # Should not raise


@patch('snowflake.core.Root')
def test_role_privilege_operations(mock_root):
    """Test role privilege grant and revoke operations"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.return_value = mock_role
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles
    mock_root.session = MagicMock()
    mock_root.session.sql = MagicMock()

    role = Role(mock_root, "DEV")

    # Create role
    config = RoleConfig(name="TEST_ROLE")
    role.create(config)

    # Grant privilege
    role.grant_privilege(
        "TEST_ROLE",
        "USAGE",
        "WAREHOUSE",
        "TEST_WH"
    )
    mock_root.session.sql.assert_called_with(
        "GRANT USAGE ON WAREHOUSE TEST_WH TO ROLE DEV_TEST_ROLE"
    )

    # Revoke privilege
    role.revoke_privilege(
        "TEST_ROLE",
        "USAGE",
        "WAREHOUSE",
        "TEST_WH"
    )
    mock_root.session.sql.assert_called_with(
        "REVOKE USAGE ON WAREHOUSE TEST_WH FROM ROLE DEV_TEST_ROLE"
    )

    # Test with non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    with pytest.raises(ValueError, match="Role TEST_ROLE does not exist"):
        role.grant_privilege("TEST_ROLE", "USAGE", "WAREHOUSE", "TEST_WH")


@patch('snowflake.core.Root')
def test_role_grant_operations(mock_root):
    """Test role grant and revoke operations"""
    # Setup mock roles
    mock_parent = MagicMock(spec=SnowflakeRole)
    mock_parent.name = "DEV_PARENT_ROLE"
    mock_parent.grant_role = MagicMock()

    mock_child = MagicMock(spec=SnowflakeRole)
    mock_child.name = "DEV_CHILD_ROLE"

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.side_effect = [mock_parent, mock_child]
    mock_roles.__getitem__.return_value = mock_parent

    # Setup mock root
    mock_root.roles = mock_roles
    mock_root.session = MagicMock()
    mock_root.session.sql = MagicMock()

    role = Role(mock_root, "DEV")

    # Create roles
    parent = role.create(RoleConfig(name="PARENT_ROLE"))
    child = role.create(RoleConfig(name="CHILD_ROLE"))

    # Grant role
    role.grant_role("PARENT_ROLE", "CHILD_ROLE")
    mock_parent.grant_role.assert_called_with(
        role_type='ROLE',
        role=RoleSecurable(name="CHILD_ROLE")
    )

    # Revoke role
    role.revoke_role("PARENT_ROLE", "CHILD_ROLE")
    mock_root.session.sql.assert_called_with(
        "REVOKE ROLE CHILD_ROLE FROM ROLE DEV_PARENT_ROLE"
    )

    # Test with non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    with pytest.raises(ValueError, match="Role PARENT_ROLE does not exist"):
        role.grant_role("PARENT_ROLE", "CHILD_ROLE")
