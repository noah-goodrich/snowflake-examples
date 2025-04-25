"""Unit tests for Role resource management."""

import pytest
from unittest.mock import MagicMock, patch
from snowflake.core._common import CreateMode
from snowflake.core.role import Role as SnowflakeRole
from resources.role import Role, RoleConfig


def test_role_config_validation():
    """Test role configuration validation"""
    # Test valid configuration
    config = RoleConfig(
        name="TEST_ROLE",
        comment="Test role"
    )
    config.validate()  # Should not raise

    # Test empty name
    with pytest.raises(ValueError, match="Role name cannot be empty"):
        RoleConfig(name="").validate()

    # Test None name
    with pytest.raises(ValueError, match="Role name cannot be empty"):
        RoleConfig(name=None).validate()


def test_role_name_formatting(snow):
    """Test role name formatting with environment prefix"""
    role = Role(snow, "DEV")

    # Test with environment prefix
    config = RoleConfig(
        name="TEST_ROLE",
        prefix_with_environment=True
    )
    assert role._format_name(config.name, True) == "DEV_TEST_ROLE"

    # Test without environment prefix
    config = RoleConfig(
        name="TEST_ROLE",
        prefix_with_environment=False
    )
    assert role._format_name(config.name, False) == "TEST_ROLE"

    # Test lowercase conversion
    config = RoleConfig(
        name="test_role",
        prefix_with_environment=True
    )
    assert role._format_name(config.name, True) == "DEV_TEST_ROLE"

    # Test mixed case handling
    config = RoleConfig(
        name="Test_Role",
        prefix_with_environment=True
    )
    assert role._format_name(config.name, True) == "DEV_TEST_ROLE"


@patch('snowflake.core.Root')
def test_role_creation(mock_root):
    """Test role creation with different modes"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"
    mock_role.comment = "Test role"

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.return_value = mock_role
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")
    config = RoleConfig(
        name="TEST_ROLE",
        comment="Test role"
    )

    # Test if_not_exists mode
    r = role.create(config, mode=CreateMode.if_not_exists)
    assert r.name == "DEV_TEST_ROLE"
    assert r.comment == "Test role"

    # Verify create was called with correct parameters
    create_call = mock_roles.create.call_args
    assert create_call is not None
    created_role, kwargs = create_call
    assert created_role[0].name == "DEV_TEST_ROLE"
    assert created_role[0].comment == "Test role"
    assert kwargs['mode'] == CreateMode.if_not_exists

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

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.create.return_value = mock_role
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Create initial role
    config = RoleConfig(
        name="TEST_ROLE",
        comment="Initial comment"
    )
    r = role.create(config)
    assert r.comment == "Initial comment"

    # Alter role
    new_config = RoleConfig(
        name="TEST_ROLE",
        comment="Updated comment"
    )
    mock_role.comment = "Updated comment"
    altered_r = role.alter("TEST_ROLE", new_config)
    assert altered_r.comment == "Updated comment"

    # Test alter of non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    with pytest.raises(ValueError, match="Role TEST_ROLE does not exist"):
        role.alter("TEST_ROLE", new_config)


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
    role.drop("NONEXISTENT_ROLE", cascade=True)  # Should not raise


@patch('snowflake.core.Root')
def test_role_get(mock_root):
    """Test role retrieval"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Test get existing role
    r = role.get("TEST_ROLE")
    assert r == mock_role
    mock_roles.__getitem__.assert_called_with("DEV_TEST_ROLE")

    # Test get non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    r = role.get("NONEXISTENT_ROLE")
    assert r is None


@patch('snowflake.core.Root')
def test_role_exists(mock_root):
    """Test role existence check"""
    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.__getitem__.return_value = MagicMock()

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Test existing role
    assert role.exists("TEST_ROLE") is True
    mock_roles.__getitem__.assert_called_with("DEV_TEST_ROLE")

    # Test non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    assert role.exists("NONEXISTENT_ROLE") is False


@patch('snowflake.core.Root')
def test_role_grant_privileges(mock_root):
    """Test role privilege grant operations"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"
    mock_role.grant_privilege = MagicMock()

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Grant privilege
    role.grant_privilege("TEST_ROLE", "USAGE", "DATABASE", "TEST_DB")
    mock_role.grant_privilege.assert_called_once_with(
        privilege="USAGE",
        on="DATABASE",
        resource="TEST_DB"
    )

    # Test grant to non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    with pytest.raises(ValueError, match="Role TEST_ROLE does not exist"):
        role.grant_privilege("TEST_ROLE", "USAGE", "DATABASE", "TEST_DB")


@patch('snowflake.core.Root')
def test_role_revoke_privileges(mock_root):
    """Test role privilege revoke operations"""
    # Setup mock role
    mock_role = MagicMock(spec=SnowflakeRole)
    mock_role.name = "DEV_TEST_ROLE"
    mock_role.revoke_privilege = MagicMock()

    # Setup mock roles collection
    mock_roles = MagicMock()
    mock_roles.__getitem__.return_value = mock_role

    # Setup mock root
    mock_root.roles = mock_roles

    role = Role(mock_root, "DEV")

    # Revoke privilege
    role.revoke_privilege("TEST_ROLE", "USAGE", "DATABASE", "TEST_DB")
    mock_role.revoke_privilege.assert_called_once_with(
        privilege="USAGE",
        on="DATABASE",
        resource="TEST_DB"
    )

    # Test revoke from non-existent role
    mock_roles.__getitem__.side_effect = KeyError()
    with pytest.raises(ValueError, match="Role TEST_ROLE does not exist"):
        role.revoke_privilege("TEST_ROLE", "USAGE", "DATABASE", "TEST_DB")
