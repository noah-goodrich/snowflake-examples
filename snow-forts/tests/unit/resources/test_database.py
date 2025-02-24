import pytest
from unittest.mock import MagicMock, patch
from resources.database import Database, DatabaseConfig
from snowflake.core._common import CreateMode
from snowflake.core.database import Database as SnowflakeDatabase
from snowflake.core.schema import Schema


def test_database_config_validation():
    """Test database configuration validation"""
    # Test valid configuration
    config = DatabaseConfig(
        name="TEST_DB",
        schemas=["SCHEMA1", "SCHEMA2"],
        comment="Test database"
    )
    config.validate()  # Should not raise

    # Test empty name
    with pytest.raises(ValueError, match="Database name cannot be empty"):
        DatabaseConfig(name="").validate()

    # Test empty schema name
    with pytest.raises(ValueError, match="Schema names cannot be empty"):
        DatabaseConfig(name="TEST_DB", schemas=["SCHEMA1", ""]).validate()


def test_database_name_formatting(snow):
    """Test database name formatting with environment prefix"""
    database = Database(snow, "DEV")

    # Test with environment prefix
    config = DatabaseConfig(name="TEST_DB", prefix_with_environment=True)
    assert database._format_name(config.name, True) == "DEV_TEST_DB"

    # Test without environment prefix
    config = DatabaseConfig(name="TEST_DB", prefix_with_environment=False)
    assert database._format_name(config.name, False) == "TEST_DB"


def test_database_creation():
    """Test database creation with different modes"""
    # Create a complete mock snow object
    snow = MagicMock()
    mock_database = MagicMock()
    mock_schemas = MagicMock()
    mock_schemas.create.return_value = MagicMock()
    mock_database.schemas = mock_schemas
    mock_database.drop = MagicMock()

    # Setup the databases collection on snow
    mock_databases = MagicMock()
    mock_databases.create.return_value = mock_database
    snow.databases = mock_databases

    database = Database(snow, "DEV")
    config = DatabaseConfig(
        name="TEST_DB",
        schemas=["SCHEMA1", "SCHEMA2"],
        comment="Test database"
    )

    # Test creation
    db = database.create(config, mode=CreateMode.if_not_exists)

    # Verify the create call
    mock_databases.create.assert_called_once()
    create_args = mock_databases.create.call_args[0][0]
    assert create_args.name == "DEV_TEST_DB"
    assert create_args.comment == "Test database"

    # Verify schema creation calls
    assert mock_schemas.create.call_count == 2
    schema_calls = mock_schemas.create.call_args_list
    assert schema_calls[0][0][0].name == "SCHEMA1"
    assert schema_calls[1][0][0].name == "SCHEMA2"


def test_database_alter(snow):
    """Test database alteration"""
    # Setup mocks
    mock_database = MagicMock()
    mock_database.schemas = MagicMock()
    mock_database.schemas.create.return_value = MagicMock()
    mock_database.drop = MagicMock()

    snow.databases.create.return_value = mock_database
    snow.databases.__getitem__.return_value = mock_database

    database = Database(snow, "DEV")

    # Create initial database
    config = DatabaseConfig(name="TEST_DB", comment="Initial comment")
    db = database.create(config)

    # Reset mock to verify alter call
    snow.databases.create.reset_mock()

    # Alter database
    new_config = DatabaseConfig(
        name="TEST_DB",
        comment="Updated comment",
        schemas=["NEW_SCHEMA"]
    )
    altered_db = database.alter("TEST_DB", new_config)

    # Verify the alter call
    snow.databases.create.assert_called_once()
    alter_args = snow.databases.create.call_args[0][0]
    assert alter_args.name == "DEV_TEST_DB"
    assert alter_args.comment == "Updated comment"


def test_database_drop(snow):
    """Test database drop operation"""
    # Setup mocks
    mock_database = MagicMock()
    mock_database.drop = MagicMock()
    snow.databases.__getitem__.return_value = mock_database

    database = Database(snow, "DEV")

    # Drop database
    database.drop("TEST_DB")

    # Verify drop was called
    mock_database.drop.assert_called_once_with(cascade=True)


def test_schema_operations(snow):
    """Test schema creation and drop operations"""
    # Setup mocks
    mock_database = MagicMock()
    mock_schema = MagicMock()
    mock_schema.drop = MagicMock()
    mock_database.schemas = MagicMock()
    mock_database.schemas.create.return_value = mock_schema
    mock_database.schemas.__getitem__.return_value = mock_schema

    snow.databases.__getitem__.return_value = mock_database

    database = Database(snow, "DEV")

    # Create schema
    schema = database.create_schema("TEST_DB", "TEST_SCHEMA")

    # Verify schema creation
    mock_database.schemas.create.assert_called_once()
    schema_args = mock_database.schemas.create.call_args[0][0]
    assert schema_args.name == "TEST_SCHEMA"

    # Drop schema
    database.drop_schema("TEST_DB", "TEST_SCHEMA", cascade=True)

    # Verify schema drop
    mock_schema.drop.assert_called_once_with(cascade=True)
