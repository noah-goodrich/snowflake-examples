"""
Pytest configuration for Snowflake testing demo.

This module provides fixtures for different testing modes:
1. Local testing using Snowpark's local testing framework
2. Live testing against actual Snowflake instance
"""

import pytest
import os
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType, StringType, DoubleType
from dotenv import load_dotenv
from typing import List, Dict, Any

# Load environment variables
load_dotenv()


def pytest_addoption(parser):
    """Add command line options for pytest."""
    parser.addoption(
        "--snowflake-session",
        action="store",
        default="local",
        choices=["local", "live"],
        help="Choose testing mode: local (local testing framework) or live (actual Snowflake)"
    )


@pytest.fixture(scope="session")
def session(request) -> Session:
    """
    Create a Snowflake session based on the testing mode.

    Args:
        request: Pytest request object

    Returns:
        Snowflake session configured for the specified testing mode
    """
    testing_mode = request.config.getoption('--snowflake-session')

    if testing_mode == "local":
        # Use Snowpark's local testing framework
        print("Creating local testing session...")
        session = Session.builder.config('local_testing', True).create()
    else:
        # Use live Snowflake connection
        print("Creating live Snowflake session...")
        connection_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "DEMO_DB"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
        }

        # Validate required parameters
        required_params = ["account", "user", "password"]
        missing_params = [
            param for param in required_params if not connection_params.get(param)]

        if missing_params:
            raise ValueError(
                f"Missing required Snowflake connection parameters: {missing_params}. "
                "Please set them in your .env file or environment variables."
            )

        session = Session.builder.configs(connection_params).create()

    # Register UDFs and stored procedures
    from src.data_processor import register_udfs_and_sprocs
    register_udfs_and_sprocs(session)

    return session


@pytest.fixture(scope="session")
def testing_mode(request) -> str:
    """Get the current testing mode."""
    return request.config.getoption('--snowflake-session')


def create_sample_data(session: Session) -> str:
    """
    Helper function to create sample data for testing.
    """
    sample_data = [
        [1, "John Doe", 1200.50, "US"],
        [2, "Jane Smith", 750.25, "EU"],
        [3, "Bob Johnson", 300.00, "CA"],
        [4, "Alice Brown", 2500.75, "AU"],
        [5, "Charlie Wilson", 450.00, "US"],
        [6, "Diana Davis", None, "EU"],  # Invalid data
        [7, "Eve Miller", 1800.00, "CA"],
        [8, "Frank Garcia", -100.00, "US"],  # Invalid data
    ]

    schema = StructType([
        StructField("customer_id", IntegerType()),
        StructField("customer_name", StringType()),
        StructField("amount", DoubleType()),
        StructField("region", StringType()),
    ])

    df = session.create_dataframe(sample_data, schema)
    table_name = "customer_data_sample"
    df.write.mode("overwrite").save_as_table(table_name)

    return table_name


def get_expected_results() -> List[Dict[str, Any]]:
    """
    Helper function to get expected results for testing.
    """
    return [
        {
            "customer_id": 1,
            "customer_name": "John Doe",
            "amount": 1200.50,
            "region": "US",
            "discount": 120.05,
            "final_amount": 1080.45,
            "customer_category": "Premium"
        },
        {
            "customer_id": 2,
            "customer_name": "Jane Smith",
            "amount": 750.25,
            "region": "EU",
            "discount": 37.51,
            "final_amount": 712.74,
            "customer_category": "Standard"
        },
        {
            "customer_id": 3,
            "customer_name": "Bob Johnson",
            "amount": 300.00,
            "region": "CA",
            "discount": 0.0,
            "final_amount": 300.00,
            "customer_category": "Basic"
        },
        {
            "customer_id": 4,
            "customer_name": "Alice Brown",
            "amount": 2500.75,
            "region": "AU",
            "discount": 250.08,
            "final_amount": 2250.67,
            "customer_category": "Premium"
        },
        {
            "customer_id": 5,
            "customer_name": "Charlie Wilson",
            "amount": 450.00,
            "region": "US",
            "discount": 0.0,
            "final_amount": 450.00,
            "customer_category": "Basic"
        },
        {
            "customer_id": 7,
            "customer_name": "Eve Miller",
            "amount": 1800.00,
            "region": "CA",
            "discount": 180.0,
            "final_amount": 1620.00,
            "customer_category": "Premium"
        }
    ]


@pytest.fixture(scope="function")
def sample_table(session, testing_mode):
    """
    Create a sample table for testing.

    Args:
        session: Snowflake session
        testing_mode: Current testing mode

    Yields:
        Table name for testing
    """
    # Create sample data
    table_name = create_sample_data(session)

    yield table_name

    # Cleanup - drop the table after test
    try:
        session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
        session.sql(f"DROP TABLE IF EXISTS {table_name}_processed").collect()
    except Exception as e:
        print(f"Warning: Could not clean up table {table_name}: {e}")


@pytest.fixture(scope="function")
def expected_results():
    """Get expected results for testing."""
    return get_expected_results()
