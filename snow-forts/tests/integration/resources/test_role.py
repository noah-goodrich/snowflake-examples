"""Integration tests for Role resource management."""

import pytest
from snowflake.core import Root
from resources.role import Role, RoleConfig
import os


@pytest.fixture
def snow():
    """Snowflake connection fixture."""
    return Root.from_connection_string(os.environ["SNOWFLAKE_CONNECTION_STRING"])


@pytest.fixture
def role_manager(snow):
    """Role manager fixture."""
    return Role(snow, "test")


def test_create_role(role_manager):
    """Test role creation."""
    # Create role
    config = RoleConfig(
        name="test_role",
        comment="Test role"
    )
    role = role_manager.create(config)

    try:
        # Verify role was created
        assert role.name == "TEST_TEST_ROLE"
        assert role.comment == "Test role"

    finally:
        # Cleanup
        role_manager.drop("test_role", cascade=True)


def test_alter_role(role_manager):
    """Test role alteration."""
    # Create initial role
    config = RoleConfig(
        name="test_role",
        comment="Initial comment"
    )
    role = role_manager.create(config)

    try:
        # Alter role
        new_config = RoleConfig(
            name="test_role",
            comment="Updated comment"
        )
        altered_role = role_manager.alter("test_role", new_config)
        assert altered_role.comment == "Updated comment"

    finally:
        # Cleanup
        role_manager.drop("test_role", cascade=True)


def test_drop_role(role_manager):
    """Test role deletion."""
    # Create role
    config = RoleConfig(name="test_role")
    role = role_manager.create(config)

    # Drop role
    role_manager.drop("test_role", cascade=True)

    # Verify role was dropped
    assert role_manager.get("test_role") is None


def test_get_role(role_manager):
    """Test role retrieval."""
    # Create role
    config = RoleConfig(
        name="test_role",
        comment="Test role"
    )
    created_role = role_manager.create(config)

    try:
        # Get role
        role = role_manager.get("test_role")
        assert role is not None
        assert role.name == created_role.name
        assert role.comment == created_role.comment

    finally:
        # Cleanup
        role_manager.drop("test_role", cascade=True)


def test_role_exists(role_manager):
    """Test role existence check."""
    # Create role
    config = RoleConfig(name="test_role")
    role = role_manager.create(config)

    try:
        # Check existence
        assert role_manager.exists("test_role") is True
        assert role_manager.exists("nonexistent_role") is False

    finally:
        # Cleanup
        role_manager.drop("test_role", cascade=True)


def test_role_privilege_operations(role_manager):
    """Test role privilege grant and revoke operations."""
    # Create role
    config = RoleConfig(name="test_role")
    role = role_manager.create(config)

    try:
        # Grant privilege
        role_manager.grant_privilege(
            "test_role",
            "USAGE",
            "DATABASE",
            "TEST_DB"
        )

        # Verify privilege was granted
        grants = role_manager.snow.session.sql(
            "SHOW GRANTS TO ROLE TEST_TEST_ROLE"
        ).collect()
        granted_privileges = [
            row['privilege'] for row in grants
            if row['granted_on'] == 'DATABASE'
            and row['name'] == 'TEST_DB'
        ]
        assert "USAGE" in granted_privileges

        # Revoke privilege
        role_manager.revoke_privilege(
            "test_role",
            "USAGE",
            "DATABASE",
            "TEST_DB"
        )

        # Verify privilege was revoked
        grants = role_manager.snow.session.sql(
            "SHOW GRANTS TO ROLE TEST_TEST_ROLE"
        ).collect()
        granted_privileges = [
            row['privilege'] for row in grants
            if row['granted_on'] == 'DATABASE'
            and row['name'] == 'TEST_DB'
        ]
        assert "USAGE" not in granted_privileges

    finally:
        # Cleanup
        role_manager.drop("test_role", cascade=True)
