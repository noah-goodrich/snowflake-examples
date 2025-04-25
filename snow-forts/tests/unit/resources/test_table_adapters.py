"""Tests for table adapters."""

import pytest
from unittest.mock import Mock, patch

from resources.table.specs import (
    ColumnSpec,
    StandardTableSpec,
    HybridTableSpec,
    DynamicTableSpec
)
from resources.table.adapters import (
    TableAdapter,
    StandardTableAdapter,
    HybridTableAdapter,
    DynamicTableAdapter
)


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
    mock.kind = "PERMANENT"
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


class TestStandardTableAdapter:
    """Tests for StandardTableAdapter."""

    def test_create_standard_table(self, mock_snow, standard_spec, mock_table):
        """Test creating a standard table."""
        # Mock the database and table creation
        mock_db = Mock()
        mock_tables = Mock()
        mock_tables.create.return_value = mock_table
        mock_db.tables = mock_tables
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and create table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema")
        result = adapter.create()

        # Verify
        assert result == mock_table
        mock_tables.create.assert_called_once()
        create_args = mock_tables.create.call_args[1]
        assert create_args["name"] == "test_table"
        assert len(create_args["columns"]) == 1
        assert create_args["columns"][0]["name"] == "id"
        assert create_args["columns"][0]["datatype"] == "NUMBER"

    def test_alter_table(self, mock_snow, standard_spec, mock_table):
        """Test altering a table."""
        # Mock database and table
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and alter table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema")
        adapter.alter()

        # Verify
        mock_table.alter.assert_called()

    def test_drop_table(self, mock_snow, standard_spec, mock_table):
        """Test dropping a table."""
        # Mock database and table
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and drop table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema")
        adapter.drop()

        # Verify
        mock_table.drop.assert_called_once()

    def test_get_table(self, mock_snow, standard_spec):
        """Test getting table details."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Test table",
            "cluster_by": "id",
            "change_tracking": False,
            "data_retention_time_in_days": 1,
            "kind": "PERMANENT"
        }]

        # Create adapter and get table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema")
        result = adapter.get()

        # Verify
        assert result is not None
        assert result["name"] == "test_table"
        mock_snow.sql.assert_called_once()


class TestHybridTableAdapter:
    """Tests for HybridTableAdapter."""

    def test_create_hybrid_table(self, mock_snow, hybrid_spec, mock_table):
        """Test creating a hybrid table."""
        # Mock the database and table creation
        mock_db = Mock()
        mock_tables = Mock()
        mock_tables.create.return_value = mock_table
        mock_db.tables = mock_tables
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and create table
        adapter = HybridTableAdapter(
            mock_snow, hybrid_spec, "test_table", "test_schema")
        result = adapter.create()

        # Verify
        assert result == mock_table
        mock_tables.create.assert_called_once()
        create_args = mock_tables.create.call_args[1]
        assert create_args["name"] == "test_table"
        assert create_args["table_type"] == "HYBRID"
        assert create_args["cluster_by"] == ["id"]

    def test_get_hybrid_table(self, mock_snow, hybrid_spec):
        """Test getting hybrid table details."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Test table",
            "cluster_by": "id",
            "change_tracking": False,
            "data_retention_time_in_days": None,
            "kind": "HYBRID"
        }]

        # Create adapter and get table
        adapter = HybridTableAdapter(
            mock_snow, hybrid_spec, "test_table", "test_schema")
        result = adapter.get()

        # Verify
        assert result is not None
        assert result["name"] == "test_table"
        assert result["kind"] == "HYBRID"
        mock_snow.sql.assert_called_once()


class TestDynamicTableAdapter:
    """Tests for DynamicTableAdapter."""

    def test_create_dynamic_table(self, mock_snow, dynamic_spec, mock_table):
        """Test creating a dynamic table."""
        # Mock the database and table retrieval after creation
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and create table
        adapter = DynamicTableAdapter(
            mock_snow, dynamic_spec, "test_table", "test_schema")
        result = adapter.create()

        # Verify
        assert result == mock_table
        mock_snow.sql.assert_called_once()
        sql_call = mock_snow.sql.call_args[0][0]
        assert "CREATE OR REPLACE DYNAMIC TABLE" in sql_call
        assert "test_schema.test_table" in sql_call
        assert "WAREHOUSE = TEST_WH" in sql_call
        assert "SELECT * FROM source_table" in sql_call

    def test_alter_dynamic_table(self, mock_snow, dynamic_spec):
        """Test altering a dynamic table."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Old comment",
            "cluster_by": "id",
            "query": "SELECT * FROM old_table",
            "kind": "DYNAMIC"
        }]

        # Create adapter and alter table
        adapter = DynamicTableAdapter(
            mock_snow, dynamic_spec, "test_table", "test_schema")
        adapter.alter()

        # Verify
        assert mock_snow.sql.call_count >= 2  # One for get(), one for alter()
        # Check the ALTER statement
        for call in mock_snow.sql.call_args_list:
            sql = call[0][0]
            if "ALTER DYNAMIC TABLE" in sql:
                assert "test_schema.test_table" in sql
                assert "SET QUERY" in sql
                break

    def test_get_dynamic_table(self, mock_snow, dynamic_spec):
        """Test getting dynamic table details."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Test table",
            "cluster_by": "id",
            "query": "SELECT * FROM source_table",
            "kind": "DYNAMIC"
        }]

        # Create adapter and get table
        adapter = DynamicTableAdapter(
            mock_snow, dynamic_spec, "test_table", "test_schema")
        result = adapter.get()

        # Verify
        assert result is not None
        assert result["name"] == "test_table"
        assert result["kind"] == "DYNAMIC"
        assert "query" in result
        mock_snow.sql.assert_called_once()
