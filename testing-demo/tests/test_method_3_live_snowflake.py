"""
Method 3: Testing by Connecting to Live Snowflake Instance

This test file demonstrates how to test Snowflake processes by connecting
to a live Snowflake instance and running tests against actual data.

Note: These tests require actual Snowflake credentials and will be skipped
when running in local mode.
"""

import pytest
import pandas as pd
from snowflake.snowpark.functions import col, call_udf
from src.data_processor import (
    validate_data,
    calculate_discount,
    calculate_final_amount,
    categorize_customers,
    process_customer_data,
    calculate_tax
)


@pytest.mark.skip(reason="Live Snowflake tests require actual credentials")
class TestLiveSnowflake:
    """Test class for live Snowflake testing approach."""

    def test_validate_data_function_live(self, session, sample_table):
        """Test the validate_data function against live Snowflake."""
        # Get input data from live Snowflake
        input_df = session.table(sample_table)

        # Apply validation
        validated_df = validate_data(input_df)

        # Check that invalid rows are filtered out
        result_count = validated_df.count()
        assert result_count == 6, f"Expected 6 valid records, got {result_count}"

        # Verify no null or negative amounts remain
        null_amounts = validated_df.filter(col("amount").is_null()).count()
        negative_amounts = validated_df.filter(col("amount") <= 0).count()

        assert null_amounts == 0, "Found null amounts after validation"
        assert negative_amounts == 0, "Found negative amounts after validation"

        # Show the results for verification
        print(f"Validated {result_count} records from live Snowflake")
        validated_df.show()

    def test_complete_stored_procedure_live(self, session, sample_table, expected_results):
        """Test the complete stored procedure against live Snowflake."""
        # Call the stored procedure on live Snowflake
        processed_count = process_customer_data(session, sample_table)

        # Verify the count of processed records
        assert processed_count == 6, f"Expected 6 processed records, got {processed_count}"

        # Get the processed data from live Snowflake
        output_table_name = f"{sample_table}_processed"
        result_df = session.table(output_table_name)

        # Convert to pandas for comparison
        result_pdf = result_df.to_pandas()
        expected_pdf = pd.DataFrame(expected_results)

        # Sort both dataframes by customer_id for comparison
        result_pdf = result_pdf.sort_values(
            'customer_id').reset_index(drop=True)
        expected_pdf = expected_pdf.sort_values(
            'customer_id').reset_index(drop=True)

        # Compare key columns
        columns_to_compare = ['customer_id', 'customer_name', 'amount', 'region',
                              'discount', 'final_amount', 'customer_category']

        for col in columns_to_compare:
            pd.testing.assert_series_equal(
                result_pdf[col],
                expected_pdf[col],
                check_dtype=False,
                check_names=False,
                rtol=0.01  # Allow small floating point differences
            )

        print(
            f"Successfully processed {processed_count} records on live Snowflake")
        result_df.show()

    def test_performance_metrics_live(self, session, sample_table):
        """Test performance metrics against live Snowflake."""
        import time

        # Measure execution time
        start_time = time.time()

        # Execute the stored procedure
        processed_count = process_customer_data(session, sample_table)

        end_time = time.time()
        execution_time = end_time - start_time

        # Verify results
        assert processed_count == 6, f"Expected 6 processed records, got {processed_count}"

        # Log performance metrics
        print(f"Live Snowflake execution time: {execution_time:.2f} seconds")
        print(f"Records processed: {processed_count}")
        print(
            f"Processing rate: {processed_count/execution_time:.2f} records/second")

        # Performance assertions (adjust thresholds as needed)
        assert execution_time < 30, f"Execution time {execution_time}s exceeds 30s threshold"

    def test_data_persistence_live(self, session, sample_table):
        """Test that data is properly persisted in live Snowflake."""
        # Process the data
        processed_count = process_customer_data(session, sample_table)

        # Verify the output table exists and has data
        output_table_name = f"{sample_table}_processed"

        # Check if table exists
        table_exists = session.sql(
            f"SHOW TABLES LIKE '{output_table_name}'").count() > 0
        assert table_exists, f"Output table {output_table_name} was not created"

        # Check table structure
        table_info = session.sql(
            f"DESCRIBE TABLE {output_table_name}").collect()
        expected_columns = ['customer_id', 'customer_name', 'amount', 'region',
                            'discount', 'final_amount', 'customer_category']

        actual_columns = [row['name'] for row in table_info]
        for col in expected_columns:
            assert col in actual_columns, f"Expected column {col} not found in output table"

        # Verify data count
        result_count = session.table(output_table_name).count()
        assert result_count == processed_count, f"Table has {result_count} rows, expected {processed_count}"

        print(
            f"Data persistence verified: {result_count} records in {output_table_name}")

    def test_warehouse_utilization_live(self, session, sample_table):
        """Test warehouse utilization during execution."""
        # Get warehouse info before execution
        warehouse_info_before = session.sql(
            "SELECT CURRENT_WAREHOUSE() as warehouse").collect()
        warehouse_name = warehouse_info_before[0]['WAREHOUSE']

        print(f"Using warehouse: {warehouse_name}")

        # Execute the stored procedure
        processed_count = process_customer_data(session, sample_table)

        # Verify execution completed successfully
        assert processed_count == 6, f"Expected 6 processed records, got {processed_count}"

        # Get warehouse status after execution
        warehouse_status = session.sql(
            f"SHOW WAREHOUSES LIKE '{warehouse_name}'").collect()

        if warehouse_status:
            status = warehouse_status[0]['state']
            print(f"Warehouse status after execution: {status}")

        print("Warehouse utilization test completed successfully")
