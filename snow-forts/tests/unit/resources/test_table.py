"""Tests for Table resource."""

import pytest
from unittest.mock import Mock, MagicMock, patch

from resources.table.specs import (
    ColumnSpec,
    StandardTableSpec,
    HybridTableSpec,
    DynamicTableSpec
)
from resources.table import Table
from snowflake.core import CreateMode


@pytest.fixture
def mock_snow():
    """Mock Snowflake root object."""
    mock = Mock()
    mock.databases = {}
    mock.sql = Mock()
    mock.sql.return_value.collect.return_value = []
    return mock


@pytest.fixture
def mock_table():
    """Mock Snowflake table object."""
    mock = Mock()
    mock.name = "test_table"
    mock.schema = "test_schema"
    mock.columns = [{"name": "id", "datatype": "NUMBER"}]
    mock.comment = "Test table"
    mock.cluster_by = ["id"]
    mock.change_tracking = False
    mock.data_retention_time_in_days = 1
    mock.drop = Mock()
    mock.alter = Mock()
    return mock


@pytest.fixture
def standard_spec():
    """Standard table specification."""
    return StandardTableSpec(
        name="test_table",
        schema="test_schema",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table",
        cluster_by=["id"],
        change_tracking=False,
        data_retention_time_in_days=1
    )


@pytest.fixture
def hybrid_spec():
    """Hybrid table specification."""
    return HybridTableSpec(
        name="test_table",
        schema="test_schema",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table",
        cluster_by=["id"]  # Required for hybrid tables
    )


@pytest.fixture
def dynamic_spec():
    """Dynamic table specification."""
    return DynamicTableSpec(
        name="test_table",
        schema="test_schema",
        database="test_db",
        columns=[
            ColumnSpec(name="id", datatype="NUMBER")
        ],
        comment="Test table",
        cluster_by=["id"],
        query="SELECT * FROM source_table",
        target_lag="10 minutes",
        warehouse="TEST_WH",
        refresh_mode="AUTO",
        initialize=True
    )


class TestTable:
    """Tests for Table class."""

    def test_create_standard_table(self, mock_snow, standard_spec, mock_table):
        """Test creating a standard table."""
        # Set up mocks
        mock_db = Mock()
        mock_tables = Mock()
        mock_tables.create.return_value = mock_table
        mock_db.tables = mock_tables
        mock_snow.databases = {"test_schema": mock_db}

        # Create and test
        table = Table(mock_snow, "dev")
        result = table.create(standard_spec)

        # Verify
        assert result == mock_table
        mock_tables.create.assert_called_once()
        create_args = mock_tables.create.call_args[1]
        assert create_args["name"] == "test_table"
        assert len(create_args["columns"]) == 1
        assert create_args["columns"][0]["name"] == "id"
        assert create_args["columns"][0]["datatype"] == "NUMBER"

    def test_create_table_with_mode(self, mock_snow, standard_spec, mock_table):
        """Test creating a table with a specific mode."""
        # Set up mocks
        mock_db = Mock()
        mock_tables = Mock()
        mock_tables.create.return_value = mock_table
        mock_db.tables = mock_tables
        mock_snow.databases = {"test_schema": mock_db}

        # Create and test
        table = Table(mock_snow, "dev")
        result = table.create(standard_spec, mode=CreateMode.failIfExists)

        # Verify
        assert result == mock_table
        mock_tables.create.assert_called_once()
        create_args = mock_tables.create.call_args[1]
        assert create_args["if_not_exists"] is False

    def test_create_hybrid_table(self, mock_snow, hybrid_spec, mock_table):
        """Test creating a hybrid table."""
        # Set up mocks
        mock_db = Mock()
        mock_tables = Mock()
        mock_tables.create.return_value = mock_table
        mock_db.tables = mock_tables
        mock_snow.databases = {"test_schema": mock_db}

        # Create and test
        table = Table(mock_snow, "dev")
        result = table.create(hybrid_spec)

        # Verify
        assert result == mock_table
        mock_tables.create.assert_called_once()
        create_args = mock_tables.create.call_args[1]
        assert create_args["name"] == "test_table"
        assert create_args["table_type"] == "HYBRID"

    def test_create_dynamic_table(self, mock_snow, dynamic_spec, mock_table):
        """Test creating a dynamic table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create and test
        table = Table(mock_snow, "dev")
        result = table.create(dynamic_spec)

        # Verify
        assert result == mock_table
        mock_snow.sql.assert_called_once()
        sql_call = mock_snow.sql.call_args[0][0]
        assert "CREATE OR REPLACE DYNAMIC TABLE" in sql_call
        assert "test_schema.test_table" in sql_call
        assert "WAREHOUSE = TEST_WH" in sql_call

    def test_get_existing_table(self, mock_snow, standard_spec, mock_table):
        """Test getting an existing table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Get and test
        table = Table(mock_snow, "dev")
        result = table.get("test_table", "test_schema")

        # Verify
        assert result == mock_table

    def test_get_nonexistent_table(self, mock_snow):
        """Test getting a nonexistent table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {}
        mock_snow.databases = {"test_schema": mock_db}

        # Get and test
        table = Table(mock_snow, "dev")
        result = table.get("nonexistent_table", "test_schema")

        # Verify
        assert result is None

    def test_table_exists(self, mock_snow, mock_table):
        """Test checking if a table exists."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Check and test
        table = Table(mock_snow, "dev")
        result = table.exists("test_table", "test_schema")

        # Verify
        assert result is True

    def test_table_does_not_exist(self, mock_snow):
        """Test checking if a nonexistent table exists."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {}
        mock_snow.databases = {"test_schema": mock_db}

        # Check and test
        table = Table(mock_snow, "dev")
        result = table.exists("nonexistent_table", "test_schema")

        # Verify
        assert result is False

    def test_alter_standard_table(self, mock_snow, standard_spec, mock_table):
        """Test altering a standard table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Alter and test
        table = Table(mock_snow, "dev")
        table.alter(standard_spec)

        # Verify
        mock_table.alter.assert_called_once()

    def test_alter_nonexistent_table(self, mock_snow, standard_spec):
        """Test altering a nonexistent table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {}
        mock_snow.databases = {"test_schema": mock_db}

        # Alter and test
        table = Table(mock_snow, "dev")
        with pytest.raises(ValueError, match="Table test_schema.test_table does not exist"):
            table.alter(standard_spec)

    def test_drop_table(self, mock_snow, mock_table):
        """Test dropping a table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Drop and test
        table = Table(mock_snow, "dev")
        table.drop("test_table", "test_schema")

        # Verify
        mock_table.drop.assert_called_once()

    def test_drop_nonexistent_table(self, mock_snow):
        """Test dropping a nonexistent table."""
        # Set up mocks
        mock_db = Mock()
        mock_db.tables = {}
        mock_snow.databases = {"test_schema": mock_db}

        # Drop and test
        table = Table(mock_snow, "dev")
        with pytest.raises(ValueError, match="Table test_schema.nonexistent_table does not exist"):
            table.drop("nonexistent_table", "test_schema")

    def test_format_name(self, mock_snow):
        """Test formatting table name."""
        table = Table(mock_snow, "dev")
        formatted_name = table._format_name("test_table")
        assert formatted_name == "dev_test_table"

    def test_format_name_no_prefix(self, mock_snow):
        """Test formatting table name without prefix."""
        table = Table(mock_snow, None)
        formatted_name = table._format_name("test_table")
        assert formatted_name == "test_table"

    def test_create_table_fail_if_exists(self):
        """Test creating a table with fail_if_exists mode."""
        table = Table()
        standard_spec = StandardTableSpec(
            name="test_table",
            columns=[ColumnSpec(name="id", datatype="INTEGER")],
            comment="Test table"
        )

        with patch.object(table, '_get_adapter') as mock_get_adapter:
            mock_adapter = Mock()
            mock_get_adapter.return_value = mock_adapter

            result = table.create(
                standard_spec, mode=CreateMode.error_if_exists)

            mock_get_adapter.assert_called_once_with(standard_spec)
            mock_adapter.create.assert_called_once_with(
                mode=CreateMode.error_if_exists)
            assert result == mock_adapter.create.return_value
