"""Tests for database resource."""

import pytest
from unittest.mock import MagicMock, patch

from snowflake.core import Root
from snowflake.core.database import Database as SnowflakeDatabase
from resources.database import Database, DatabaseConfig


@patch('snowflake.core.Root')
def test_database_create(mock_root):
    """Test database creation"""
    # Setup mock database
    mock_db = MagicMock(spec=SnowflakeDatabase)
    mock_db.name = "DEV_TEST_DB"
    mock_db.comment = "Test database"

    # Setup mock databases collection
    mock_dbs = MagicMock()
    mock_dbs.create.return_value = mock_db

    # Setup mock root
    mock_root.databases = mock_dbs

    # Create database
    config = DatabaseConfig(
        name="TEST_DB",
        comment="Test database"
    )
    database = Database(mock_root, "DEV", config)
    db = database.create()
    assert db.comment == "Test database"
    mock_dbs.create.assert_called_once_with(
        "DEV_TEST_DB",
        comment="Test database",
        data_retention_time_in_days=None
    )


@patch('snowflake.core.Root')
def test_database_alter(mock_root):
    """Test database alteration"""
    # Setup mock database
    mock_db = MagicMock(spec=SnowflakeDatabase)
    mock_db.name = "DEV_TEST_DB"
    mock_db.comment = "Initial comment"

    # Setup mock databases collection
    mock_dbs = MagicMock()
    mock_dbs.create.return_value = mock_db
    mock_dbs.__getitem__.return_value = mock_db
    mock_dbs.alter.return_value = mock_db

    # Setup mock root
    mock_root.databases = mock_dbs

    # Create initial database
    config = DatabaseConfig(
        name="TEST_DB",
        comment="Initial comment"
    )
    database = Database(mock_root, "DEV", config)
    db = database.create()
    assert db.comment == "Initial comment"

    # Alter database
    new_config = DatabaseConfig(
        name="TEST_DB",
        comment="Updated comment"
    )
    mock_db.comment = "Updated comment"
    altered_db = database.alter(new_config)
    assert altered_db.comment == "Updated comment"
    mock_dbs.alter.assert_called_once_with(
        "DEV_TEST_DB",
        comment="Updated comment",
        data_retention_time_in_days=None
    )


@patch('snowflake.core.Root')
def test_database_drop(mock_root):
    """Test database drop operation"""
    # Setup mock database
    mock_db = MagicMock(spec=SnowflakeDatabase)
    mock_db.name = "DEV_TEST_DB"
    mock_db.drop = MagicMock()

    # Setup mock databases collection
    mock_dbs = MagicMock()
    mock_dbs.create.return_value = mock_db
    mock_dbs.__getitem__.return_value = mock_db

    # Setup mock root
    mock_root.databases = mock_dbs

    # Create database
    config = DatabaseConfig(name="TEST_DB")
    database = Database(mock_root, "DEV", config)
    db = database.create()

    # Drop database
    database.drop(cascade=True)
    mock_dbs.drop.assert_called_once_with("DEV_TEST_DB", cascade=True)


@patch('snowflake.core.Root')
def test_database_get(mock_root):
    """Test database retrieval"""
    # Setup mock database
    mock_db = MagicMock(spec=SnowflakeDatabase)
    mock_db.name = "DEV_TEST_DB"

    # Setup mock databases collection
    mock_dbs = MagicMock()
    mock_dbs.__getitem__.return_value = mock_db

    # Setup mock root
    mock_root.databases = mock_dbs

    # Test get existing database
    config = DatabaseConfig(name="TEST_DB")
    database = Database(mock_root, "DEV", config)
    db = database.get()
    assert db == mock_db
    mock_dbs.__getitem__.assert_called_once_with("DEV_TEST_DB")


@patch('snowflake.core.Root')
def test_database_exists(mock_root):
    """Test database existence check"""
    # Setup mock databases collection
    mock_dbs = MagicMock()
    mock_dbs.__getitem__.return_value = MagicMock()

    # Setup mock root
    mock_root.databases = mock_dbs

    # Test existing database
    config = DatabaseConfig(name="TEST_DB")
    database = Database(mock_root, "DEV", config)
    assert database.exists() is True
    mock_dbs.__getitem__.assert_called_once_with("DEV_TEST_DB")


def test_database_config_validation():
    """Test database configuration validation"""
    # Test valid config
    config = DatabaseConfig(name="TEST_DB")
    config.validate()  # Should not raise

    # Test invalid config
    with pytest.raises(ValueError):
        DatabaseConfig(name="").validate()


@patch('snowflake.core.Root')
def test_database_name_formatting(mock_root):
    """Test database name formatting with environment prefix"""
    # Test with environment prefix
    config = DatabaseConfig(
        name="TEST_DB",
        prefix_with_environment=True
    )
    database = Database(mock_root, "DEV", config)
    assert database._name == "DEV_TEST_DB"

    # Test without environment prefix
    config = DatabaseConfig(
        name="TEST_DB",
        prefix_with_environment=False
    )
    database = Database(mock_root, "DEV", config)
    assert database._name == "TEST_DB"

    # Test lowercase conversion
    config = DatabaseConfig(
        name="test_db",
        prefix_with_environment=True
    )
    database = Database(mock_root, "DEV", config)
    assert database._name == "DEV_TEST_DB"

    # Test mixed case handling
    config = DatabaseConfig(
        name="Test_Db",
        prefix_with_environment=True
    )
    database = Database(mock_root, "DEV", config)
    assert database._name == "DEV_TEST_DB"
