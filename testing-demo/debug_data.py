"""
Debug script to understand data behavior in local testing framework.
"""

from snowflake.snowpark.session import Session
from src.data_processor import validate_data
from tests.conftest import create_sample_data

# Create local session
session = Session.builder.config('local_testing', True).create()

# Create sample data
table_name = create_sample_data(session)

# Get the data
input_df = session.table(table_name)
print(f"Original data count: {input_df.count()}")

# Show the data
print("Original data:")
input_df.show()

# Apply validation
validated_df = validate_data(input_df)
print(f"Validated data count: {validated_df.count()}")

# Show validated data
print("Validated data:")
validated_df.show()

# Check for null amounts
null_amounts = input_df.filter(input_df["amount"].is_null()).count()
print(f"Null amounts: {null_amounts}")

# Check for negative amounts
negative_amounts = input_df.filter(input_df["amount"] <= 0).count()
print(f"Negative amounts: {negative_amounts}")

# Check for null customer_ids
null_customer_ids = input_df.filter(input_df["customer_id"].is_null()).count()
print(f"Null customer IDs: {null_customer_ids}")
