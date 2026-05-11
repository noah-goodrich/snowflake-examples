"""
Method 1: Testing using Snowpark's Local Testing Framework

This test file demonstrates how to test Snowflake processes using the
local testing framework provided by Snowpark Python. This allows testing
without connecting to a live Snowflake instance.
"""

import pytest
import pandas as pd
from snowflake.snowpark.functions import col
from src.data_processor import (
    validate_data,
    calculate_discount,
    calculate_final_amount,
    categorize_customers,
    process_customer_data,
    calculate_tax
)


class TestLocalTestingFramework:
    """Test class for local testing framework approach."""

    def test_validate_data_function(self, session, sample_table):
        """Test the validate_data internal function."""
        # Get input data
        input_df = session.table(sample_table)

        # Apply validation
        validated_df = validate_data(input_df)

        # Check that invalid rows are filtered out
        # Should filter out customer_id 6 (null amount) and 8 (negative amount)
        result_count = validated_df.count()
        assert result_count == 6, f"Expected 6 valid records, got {result_count}"

        # Verify no null or negative amounts remain
        null_amounts = validated_df.filter(col("amount").is_null()).count()
        negative_amounts = validated_df.filter(col("amount") <= 0).count()

        assert null_amounts == 0, "Found null amounts after validation"
        assert negative_amounts == 0, "Found negative amounts after validation"

    def test_calculate_discount_function(self, session, sample_table):
        """Test the calculate_discount internal function."""
        # Get validated data
        input_df = session.table(sample_table)
        validated_df = validate_data(input_df)

        # Apply discount calculation
        df_with_discount = calculate_discount(validated_df)

        # Use collect() instead of to_pandas() for local testing
        result_data = df_with_discount.collect()

        # Convert to list of dictionaries for easier testing
        result_records = [row.as_dict() for row in result_data]

        # Test discount calculations
        premium_customer = next(
            r for r in result_records if r['CUSTOMER_ID'] == 1)
        standard_customer = next(
            r for r in result_records if r['CUSTOMER_ID'] == 2)
        basic_customer = next(
            r for r in result_records if r['CUSTOMER_ID'] == 3)

        # Premium customer (amount >= 1000): 10% discount
        assert abs(premium_customer['DISCOUNT'] -
                   120.05) < 0.01, f"Expected discount ~120.05, got {premium_customer['DISCOUNT']}"

        # Standard customer (amount >= 500): 5% discount
        assert abs(standard_customer['DISCOUNT'] -
                   37.51) < 0.01, f"Expected discount ~37.51, got {standard_customer['DISCOUNT']}"

        # Basic customer (amount < 500): 0% discount
        assert basic_customer[
            'DISCOUNT'] == 0, f"Expected discount 0, got {basic_customer['DISCOUNT']}"

    def test_calculate_final_amount_function(self, session, sample_table):
        """Test the calculate_final_amount internal function."""
        # Get data with discounts
        input_df = session.table(sample_table)
        validated_df = validate_data(input_df)
        df_with_discount = calculate_discount(validated_df)

        # Apply final amount calculation
        df_with_final_amount = calculate_final_amount(df_with_discount)

        # Use collect() instead of to_pandas() for local testing
        result_data = df_with_final_amount.collect()
        result_records = [row.as_dict() for row in result_data]

        # Test final amount calculations
        premium_customer = next(
            r for r in result_records if r['CUSTOMER_ID'] == 1)
        basic_customer = next(
            r for r in result_records if r['CUSTOMER_ID'] == 3)

        # Premium customer: amount - discount
        expected_final = 1200.50 - 120.05
        assert abs(premium_customer['FINAL_AMOUNT'] - expected_final) < 0.01, \
            f"Expected final amount ~{expected_final}, got {premium_customer['FINAL_AMOUNT']}"

        # Basic customer: amount - 0 discount
        assert basic_customer['FINAL_AMOUNT'] == 300.00, \
            f"Expected final amount 300.00, got {basic_customer['FINAL_AMOUNT']}"

    def test_categorize_customers_function(self, session, sample_table):
        """Test the categorize_customers internal function."""
        # Get validated data
        input_df = session.table(sample_table)
        validated_df = validate_data(input_df)

        # Apply customer categorization
        categorized_df = categorize_customers(validated_df)

        # Use collect() instead of to_pandas() for local testing
        result_data = categorized_df.collect()
        result_records = [row.as_dict() for row in result_data]

        # Test categorization
        premium_customers = [
            r for r in result_records if r['CUSTOMER_CATEGORY'] == 'Premium']
        standard_customers = [
            r for r in result_records if r['CUSTOMER_CATEGORY'] == 'Standard']
        basic_customers = [
            r for r in result_records if r['CUSTOMER_CATEGORY'] == 'Basic']

        assert len(
            premium_customers) == 3, f"Expected 3 Premium customers, got {len(premium_customers)}"
        assert len(
            standard_customers) == 1, f"Expected 1 Standard customer, got {len(standard_customers)}"
        assert len(
            basic_customers) == 2, f"Expected 2 Basic customers, got {len(basic_customers)}"

        # Verify specific customer categories
        customer_1 = next(r for r in result_records if r['CUSTOMER_ID'] == 1)
        customer_2 = next(r for r in result_records if r['CUSTOMER_ID'] == 2)
        customer_3 = next(r for r in result_records if r['CUSTOMER_ID'] == 3)

        assert customer_1['CUSTOMER_CATEGORY'] == 'Premium'
        assert customer_2['CUSTOMER_CATEGORY'] == 'Standard'
        assert customer_3['CUSTOMER_CATEGORY'] == 'Basic'

    def test_complete_stored_procedure(self, session, sample_table, expected_results):
        """Test the complete stored procedure end-to-end."""
        # Call the stored procedure
        processed_count = process_customer_data(session, sample_table)

        # Verify the count of processed records
        assert processed_count == 6, f"Expected 6 processed records, got {processed_count}"

        # Get the processed data
        output_table_name = f"{sample_table}_processed"
        result_df = session.table(output_table_name)

        # Use collect() instead of to_pandas() for local testing
        result_data = result_df.collect()
        result_records = [row.as_dict() for row in result_data]

        # Sort by customer_id for comparison
        result_records.sort(key=lambda x: x['CUSTOMER_ID'])

        # Compare with expected results (adjust for uppercase column names)
        expected_records = []
        for expected in expected_results:
            expected_records.append({
                'CUSTOMER_ID': expected['customer_id'],
                'CUSTOMER_NAME': expected['customer_name'],
                'AMOUNT': expected['amount'],
                'REGION': expected['region'],
                'DISCOUNT': expected['discount'],
                'FINAL_AMOUNT': expected['final_amount'],
                'CUSTOMER_CATEGORY': expected['customer_category']
            })

        # Compare key fields
        for i, (result, expected) in enumerate(zip(result_records, expected_records)):
            assert result['CUSTOMER_ID'] == expected[
                'CUSTOMER_ID'], f"Customer ID mismatch at index {i}"
            assert abs(result['DISCOUNT'] - expected['DISCOUNT']
                       ) < 0.01, f"Discount mismatch at index {i}"
            assert abs(result['FINAL_AMOUNT'] - expected['FINAL_AMOUNT']
                       ) < 0.01, f"Final amount mismatch at index {i}"
            assert result['CUSTOMER_CATEGORY'] == expected[
                'CUSTOMER_CATEGORY'], f"Category mismatch at index {i}"

    def test_udf_function(self, session):
        """Test the UDF function."""
        # Test tax calculation for different regions
        test_cases = [
            (100.0, "US", 8.0),    # 8% tax
            (100.0, "EU", 20.0),   # 20% tax
            (100.0, "CA", 13.0),   # 13% tax
            (100.0, "AU", 10.0),   # 10% tax
            (100.0, "UK", 5.0),    # Default 5% tax
        ]

        for amount, region, expected_tax in test_cases:
            calculated_tax = calculate_tax(amount, region)
            assert abs(calculated_tax - expected_tax) < 0.01, \
                f"Expected tax {expected_tax} for {region}, got {calculated_tax}"

    def test_error_handling(self, session):
        """Test error handling in the stored procedure."""
        # Try to process a non-existent table
        with pytest.raises(Exception):
            process_customer_data(session, "non_existent_table")

    def test_data_integrity(self, session, sample_table):
        """Test data integrity throughout the process."""
        # Get original data
        original_df = session.table(sample_table)
        original_count = original_df.count()

        # Process the data
        processed_count = process_customer_data(session, sample_table)

        # Verify that processed count is less than or equal to original count
        # (due to validation filtering out invalid records)
        assert processed_count <= original_count, \
            f"Processed count ({processed_count}) should not exceed original count ({original_count})"

        # Verify that all processed records have required columns
        output_table_name = f"{sample_table}_processed"
        result_df = session.table(output_table_name)

        required_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'AMOUNT', 'REGION',
                            'DISCOUNT', 'FINAL_AMOUNT', 'CUSTOMER_CATEGORY']

        result_columns = result_df.columns
        for col in required_columns:
            assert col in result_columns, f"Required column '{col}' missing from output"
