"""Integration tests for Database resource management."""

import pytest
from snowflake.core import Root
from resources.database import Database, DatabaseConfig
import os


@pytest.fixture
def snow():
    """Snowflake connection fixture."""
    return Root.from_connection_string(os.environ["SNOWFLAKE_CONNECTION_STRING"])


@pytest.fixture
def database_manager(snow):
    """Database manager fixture."""
    return Database(snow, "test")


def test_create_database(database_manager):
    """Test database creation."""
    # Create database
    config = DatabaseConfig(
        name="test_db",
        comment="Test database"
    )
    db = database_manager.create(config)

    try:
        # Verify database was created
        assert db.name == "TEST_TEST_DB"
        assert db.comment == "Test database"

    finally:
        # Cleanup
        database_manager.drop("test_db", cascade=True)


def test_alter_database(database_manager):
    """Test database alteration."""
    # Create initial database
    config = DatabaseConfig(
        name="test_db",
        comment="Initial comment"
    )
    db = database_manager.create(config)

    try:
        # Alter database
        new_config = DatabaseConfig(
            name="test_db",
            comment="Updated comment"
        )
        altered_db = database_manager.alter("test_db", new_config)
        assert altered_db.comment == "Updated comment"

    finally:
        # Cleanup
        database_manager.drop("test_db", cascade=True)


def test_drop_database(database_manager):
    """Test database deletion."""
    # Create database
    config = DatabaseConfig(name="test_db")
    db = database_manager.create(config)

    # Drop database
    database_manager.drop("test_db", cascade=True)

    # Verify database was dropped
    assert database_manager.get("test_db") is None


def test_get_database(database_manager):
    """Test database retrieval."""
    # Create database
    config = DatabaseConfig(
        name="test_db",
        comment="Test database"
    )
    created_db = database_manager.create(config)

    try:
        # Get database
        db = database_manager.get("test_db")
        assert db is not None
        assert db.name == created_db.name
        assert db.comment == created_db.comment

    finally:
        # Cleanup
        database_manager.drop("test_db", cascade=True)


def test_database_exists(database_manager):
    """Test database existence check."""
    # Create database
    config = DatabaseConfig(name="test_db")
    db = database_manager.create(config)

    try:
        # Check existence
        assert database_manager.exists("test_db") is True
        assert database_manager.exists("nonexistent_db") is False

    finally:
        # Cleanup
        database_manager.drop("test_db", cascade=True)
