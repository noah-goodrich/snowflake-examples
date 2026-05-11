"""
Data Processing Module for Snowflake Testing Demo

This module contains a stored procedure and internal functions that demonstrate
various data transformation operations that we'll test using different methods.
"""

from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import col, when, lit, udf, sproc, call_udf
from snowflake.snowpark.types import IntegerType, StringType, DoubleType, StructType, StructField
from snowflake.snowpark.dataframe import DataFrame
import pandas as pd
from typing import List, Dict, Any


def validate_data(df: DataFrame) -> DataFrame:
    """
    Internal function to validate input data.
    Removes rows with null values in critical columns.
    """
    # More explicit validation to handle local testing framework behavior
    return df.filter(
        col("customer_id").is_not_null() &
        col("amount").is_not_null() &
        (col("amount") > 0)  # Explicitly check for positive amounts
    )


def calculate_discount(df: DataFrame) -> DataFrame:
    """
    Internal function to calculate discount based on amount.
    """
    return df.with_column(
        "discount",
        when(col("amount") >= 1000, col("amount") * 0.1)
        .when(col("amount") >= 500, col("amount") * 0.05)
        .otherwise(lit(0))
    )


def calculate_final_amount(df: DataFrame) -> DataFrame:
    """
    Internal function to calculate final amount after discount.
    """
    return df.with_column(
        "final_amount",
        col("amount") - col("discount")
    )


def categorize_customers(df: DataFrame) -> DataFrame:
    """
    Internal function to categorize customers based on spending.
    """
    return df.with_column(
        "customer_category",
        when(col("amount") >= 1000, "Premium")
        .when(col("amount") >= 500, "Standard")
        .otherwise("Basic")
    )


def process_customer_data(session: Session, table_name: str) -> int:
    """
    Main stored procedure that processes customer data.

    This procedure:
    1. Reads customer data from the specified table
    2. Validates the data
    3. Calculates discounts
    4. Calculates final amounts
    5. Categorizes customers
    6. Saves the processed data to a new table

    Args:
        session: Snowflake session
        table_name: Name of the input table

    Returns:
        Number of processed records
    """
    try:
        # Read the input data
        input_df = session.table(table_name)

        # Apply transformations using internal functions
        validated_df = validate_data(input_df)
        df_with_discount = calculate_discount(validated_df)
        df_with_final_amount = calculate_final_amount(df_with_discount)
        final_df = categorize_customers(df_with_final_amount)

        # Save the processed data
        output_table_name = f"{table_name}_processed"
        final_df.write.mode("overwrite").save_as_table(output_table_name)

        # Return the count of processed records
        return final_df.count()

    except Exception as e:
        print(f"Error in process_customer_data: {str(e)}")
        raise


def calculate_tax(amount: float, region: str) -> float:
    """
    UDF to calculate tax based on region.
    """
    tax_rates = {
        "US": 0.08,
        "EU": 0.20,
        "CA": 0.13,
        "AU": 0.10
    }
    return amount * tax_rates.get(region, 0.05)


def register_udfs_and_sprocs(session: Session):
    """
    Register UDFs and stored procedures with the session.
    This should be called after creating a session.
    """
    # Register the UDF
    @udf(name='calculate_tax', return_type=DoubleType(), input_types=[DoubleType(), StringType()])
    def calculate_tax_udf(amount: float, region: str) -> float:
        return calculate_tax(amount, region)

    # Register the stored procedure
    @sproc(name='process_customer_data', return_type=IntegerType(), input_types=[StringType()])
    def process_customer_data_sproc(session: Session, table_name: str) -> int:
        return process_customer_data(session, table_name)
