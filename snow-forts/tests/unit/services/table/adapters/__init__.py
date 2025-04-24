"""Tests for table adapters."""

import pytest
from unittest.mock import Mock, patch, MagicMock

from snowflake.core import Root, CreateMode
from snowflake.core.table import Table as SnowflakeTable

from specs.table import (
    ColumnSpec,
    ColumnOperation,
    StandardTableSpec,
    HybridTableSpec,
    DynamicTableSpec
)

from services.table.adapters import (
    TableAdapter,
    StandardTableAdapter,
    HybridTableAdapter,
    DynamicTableAdapter
)


@pytest.fixture
def mock_snow():
    """Mock Snowflake root object."""
    mock = Mock(spec=Root)
    mock.databases = {}
    mock.sql = Mock()
    mock.sql.return_value.collect.return_value = []
    return mock


@pytest.fixture
def mock_table():
    """Mock Snowflake table object."""
    mock = Mock(spec=SnowflakeTable)
    mock.name = "test_table"
    mock.schema = "test_schema"
    mock.columns = [{"name": "id", "datatype": "NUMBER"}]
    mock.comment = "Test table"
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
        comment="Test table"
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
        cluster_by=["id"],
        auto_clustering=True
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


class TestTableAdapter:
    """Tests for the TableAdapter base class."""

    def test_create_for_spec_standard(self, mock_snow, standard_spec):
        """Test creating adapter for standard table spec."""
        with patch('services.table.adapters.StandardTableAdapter') as mock_adapter_class:
            mock_adapter = Mock(spec=StandardTableAdapter)
            mock_adapter_class.return_value = mock_adapter

            adapter = TableAdapter.create_for_spec(
                mock_snow, standard_spec, "test_table", "test_schema"
            )

            mock_adapter_class.assert_called_once_with(
                mock_snow, standard_spec, "test_table", "test_schema"
            )
            assert adapter == mock_adapter

    def test_create_for_spec_hybrid(self, mock_snow, hybrid_spec):
        """Test creating adapter for hybrid table spec."""
        with patch('services.table.adapters.HybridTableAdapter') as mock_adapter_class:
            mock_adapter = Mock(spec=HybridTableAdapter)
            mock_adapter_class.return_value = mock_adapter

            adapter = TableAdapter.create_for_spec(
                mock_snow, hybrid_spec, "test_table", "test_schema"
            )

            mock_adapter_class.assert_called_once_with(
                mock_snow, hybrid_spec, "test_table", "test_schema"
            )
            assert adapter == mock_adapter

    def test_create_for_spec_dynamic(self, mock_snow, dynamic_spec):
        """Test creating adapter for dynamic table spec."""
        with patch('services.table.adapters.DynamicTableAdapter') as mock_adapter_class:
            mock_adapter = Mock(spec=DynamicTableAdapter)
            mock_adapter_class.return_value = mock_adapter

            adapter = TableAdapter.create_for_spec(
                mock_snow, dynamic_spec, "test_table", "test_schema"
            )

            mock_adapter_class.assert_called_once_with(
                mock_snow, dynamic_spec, "test_table", "test_schema"
            )
            assert adapter == mock_adapter

    def test_get_qualified_name(self, mock_snow, standard_spec):
        """Test getting qualified name."""
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )
        qualified_name = adapter.get_qualified_name()
        assert qualified_name == "test_schema.test_table"

    def test_exists_true(self, mock_snow, standard_spec):
        """Test checking if table exists when it does."""
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )

        with patch.object(adapter, 'get') as mock_get:
            mock_get.return_value = {"name": "test_table"}
            assert adapter.exists() is True
            mock_get.assert_called_once()

    def test_exists_false(self, mock_snow, standard_spec):
        """Test checking if table exists when it does not."""
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )

        with patch.object(adapter, 'get') as mock_get:
            mock_get.return_value = None
            assert adapter.exists() is False
            mock_get.assert_called_once()


class TestStandardTableAdapter:
    """Tests for the StandardTableAdapter."""

    def test_create(self, mock_snow, standard_spec, mock_table):
        """Test creating a standard table."""
        # Mock the database and table creation
        mock_db = Mock()
        mock_db.tables = Mock()
        mock_db.tables.create.return_value = mock_table
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and create table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )

        result = adapter.create()

        # Verify
        assert result == mock_table
        mock_db.tables.create.assert_called_once()
        create_args = mock_db.tables.create.call_args[1]
        assert create_args["name"] == "test_table"
        assert len(create_args["columns"]) == 1
        assert create_args["columns"][0]["name"] == "id"
        assert create_args["columns"][0]["datatype"] == "NUMBER"

    def test_alter(self, mock_snow, standard_spec, mock_table):
        """Test altering a standard table."""
        # Mock the database and table
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and alter table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )

        # Define column operations
        column_ops = [
            ColumnOperation(
                operation="ADD",
                column_name="new_column",
                datatype="VARCHAR"
            )
        ]

        adapter.alter(column_ops)

        # Verify
        mock_table.alter.assert_called_once()
        alter_args = mock_table.alter.call_args[1]
        assert "add_columns" in alter_args

    def test_drop(self, mock_snow, standard_spec, mock_table):
        """Test dropping a standard table."""
        # Mock the database and table
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and drop table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )

        adapter.drop()

        # Verify
        mock_table.drop.assert_called_once()

    def test_get(self, mock_snow, standard_spec):
        """Test getting standard table details."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Test table",
            "cluster_by": None,
            "kind": "STANDARD"
        }]

        # Create adapter and get table
        adapter = StandardTableAdapter(
            mock_snow, standard_spec, "test_table", "test_schema"
        )

        result = adapter.get()

        # Verify
        assert result is not None
        assert result["name"] == "test_table"
        assert result["schema_name"] == "test_schema"
        assert result["kind"] == "STANDARD"
        mock_snow.sql.assert_called_once()


class TestHybridTableAdapter:
    """Tests for the HybridTableAdapter."""

    def test_create(self, mock_snow, hybrid_spec, mock_table):
        """Test creating a hybrid table."""
        # Mock the database and table creation
        mock_db = Mock()
        mock_db.tables = Mock()
        mock_db.tables.create.return_value = mock_table
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and create table
        adapter = HybridTableAdapter(
            mock_snow, hybrid_spec, "test_table", "test_schema"
        )

        result = adapter.create()

        # Verify
        assert result == mock_table
        mock_db.tables.create.assert_called_once()
        create_args = mock_db.tables.create.call_args[1]
        assert create_args["name"] == "test_table"
        assert create_args["table_type"] == "HYBRID"
        assert create_args["auto_clustering"] is True
        assert create_args["clustering_key"] == ["id"]

    def test_alter(self, mock_snow, hybrid_spec, mock_table):
        """Test altering a hybrid table."""
        # Mock the database and table
        mock_db = Mock()
        mock_db.tables = {"test_table": mock_table}
        mock_snow.databases = {"test_schema": mock_db}

        # Create adapter and alter table
        adapter = HybridTableAdapter(
            mock_snow, hybrid_spec, "test_table", "test_schema"
        )

        # Define column operations
        column_ops = [
            ColumnOperation(
                operation="ADD",
                column_name="new_column",
                datatype="VARCHAR"
            )
        ]

        adapter.alter(column_ops)

        # Verify
        mock_table.alter.assert_called_once()
        alter_args = mock_table.alter.call_args[1]
        assert "add_columns" in alter_args

    def test_get(self, mock_snow, hybrid_spec):
        """Test getting hybrid table details."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Test table",
            "cluster_by": "id",
            "kind": "HYBRID"
        }]

        # Create adapter and get table
        adapter = HybridTableAdapter(
            mock_snow, hybrid_spec, "test_table", "test_schema"
        )

        result = adapter.get()

        # Verify
        assert result is not None
        assert result["name"] == "test_table"
        assert result["schema_name"] == "test_schema"
        assert result["kind"] == "HYBRID"
        assert result["cluster_by"] == "id"
        mock_snow.sql.assert_called_once()


class TestDynamicTableAdapter:
    """Tests for the DynamicTableAdapter."""

    def test_create(self, mock_snow, dynamic_spec):
        """Test creating a dynamic table."""
        # Mock SQL execution
        mock_snow.sql.return_value.collect.return_value = []

        # Create adapter and create table
        adapter = DynamicTableAdapter(
            mock_snow, dynamic_spec, "test_table", "test_schema"
        )

        # Mock get_snowflake_table to return a table
        mock_table = Mock(spec=SnowflakeTable)
        with patch.object(adapter, 'get_snowflake_table', return_value=mock_table):
            result = adapter.create()

            # Verify
            assert result == mock_table
            mock_snow.sql.assert_called_once()
            sql_call = mock_snow.sql.call_args[0][0]
            assert "CREATE OR REPLACE DYNAMIC TABLE" in sql_call
            assert "test_schema.test_table" in sql_call
            assert "TARGET_LAG" in sql_call
            assert "WAREHOUSE = TEST_WH" in sql_call
            assert "REFRESH_MODE = AUTO" in sql_call

    def test_alter(self, mock_snow, dynamic_spec):
        """Test altering a dynamic table."""
        # Mock SQL execution
        mock_snow.sql.return_value.collect.return_value = []

        # Create adapter and alter table
        adapter = DynamicTableAdapter(
            mock_snow, dynamic_spec, "test_table", "test_schema"
        )

        adapter.alter()

        # Verify
        mock_snow.sql.assert_called_once()
        sql_call = mock_snow.sql.call_args[0][0]
        assert "ALTER DYNAMIC TABLE" in sql_call
        assert "test_schema.test_table" in sql_call

    def test_get(self, mock_snow, dynamic_spec):
        """Test getting dynamic table details."""
        # Mock SQL result
        mock_snow.sql.return_value.collect.return_value = [{
            "name": "test_table",
            "schema_name": "test_schema",
            "comment": "Test table",
            "cluster_by": "id",
            "query": "SELECT * FROM source_table",
            "kind": "DYNAMIC",
            "target_lag": "10 minutes",
            "warehouse": "TEST_WH"
        }]

        # Create adapter and get table
        adapter = DynamicTableAdapter(
            mock_snow, dynamic_spec, "test_table", "test_schema"
        )

        result = adapter.get()

        # Verify
        assert result is not None
        assert result["name"] == "test_table"
        assert result["schema_name"] == "test_schema"
        assert result["kind"] == "DYNAMIC"
        assert result["query"] == "SELECT * FROM source_table"
        mock_snow.sql.assert_called_once()
