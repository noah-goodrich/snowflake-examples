"""
Demo script for Live Snowflake Testing

This script demonstrates how to test Snowflake processes by connecting
to a live Snowflake instance. This requires actual Snowflake credentials.

To use this script:
1. Set up your Snowflake credentials in a .env file or environment variables
2. Uncomment the live session creation code
3. Run the script to see live Snowflake testing in action
"""

import os
from dotenv import load_dotenv
from snowflake.snowpark.session import Session
from src.data_processor import process_customer_data, register_udfs_and_sprocs
from tests.conftest import create_sample_data, get_expected_results

# Load environment variables
load_dotenv()


def demo_live_snowflake_testing():
    """
    Demo function showing how to test against live Snowflake.
    """
    print("=== Live Snowflake Testing Demo ===\n")
    
    # Check if we have the required credentials
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Missing required Snowflake credentials:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nTo run this demo:")
        print("1. Create a .env file with your Snowflake credentials:")
        print("   SNOWFLAKE_ACCOUNT=your_account")
        print("   SNOWFLAKE_USER=your_username")
        print("   SNOWFLAKE_PASSWORD=your_password")
        print("   SNOWFLAKE_WAREHOUSE=your_warehouse")
        print("   SNOWFLAKE_DATABASE=your_database")
        print("   SNOWFLAKE_SCHEMA=your_schema")
        print("   SNOWFLAKE_ROLE=your_role")
        print("\n2. Uncomment the live session creation code in this script")
        print("3. Run the script again")
        return
    
    print("✅ Snowflake credentials found!")
    print("Note: This demo is currently disabled to prevent accidental execution.")
    print("Uncomment the code below to run live Snowflake tests.\n")
    
    # Uncomment the following code to run live Snowflake tests:
    """
    # Create live Snowflake session
    print("🔗 Connecting to live Snowflake...")
    connection_params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "DEMO_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    }
    
    session = Session.builder.configs(connection_params).create()
    print("✅ Connected to Snowflake successfully!")
    
    # Register UDFs and stored procedures
    print("📝 Registering UDFs and stored procedures...")
    register_udfs_and_sprocs(session)
    print("✅ Registration complete!")
    
    # Create sample data
    print("📊 Creating sample data...")
    table_name = create_sample_data(session)
    print(f"✅ Sample data created in table: {table_name}")
    
    # Test the stored procedure
    print("🚀 Testing stored procedure...")
    import time
    start_time = time.time()
    
    processed_count = process_customer_data(session, table_name)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"✅ Stored procedure completed!")
    print(f"   - Records processed: {processed_count}")
    print(f"   - Execution time: {execution_time:.2f} seconds")
    print(f"   - Processing rate: {processed_count/execution_time:.2f} records/second")
    
    # Verify results
    print("🔍 Verifying results...")
    output_table_name = f"{table_name}_processed"
    result_df = session.table(output_table_name)
    
    # Show the processed data
    print("📋 Processed data:")
    result_df.show()
    
    # Check table structure
    print("📋 Table structure:")
    table_info = session.sql(f"DESCRIBE TABLE {output_table_name}").collect()
    for row in table_info:
        print(f"   - {row['name']}: {row['type']}")
    
    # Performance metrics
    print("\n📈 Performance Metrics:")
    print(f"   - Warehouse: {session.sql('SELECT CURRENT_WAREHOUSE()').collect()[0][0]}")
    print(f"   - Database: {session.sql('SELECT CURRENT_DATABASE()').collect()[0][0]}")
    print(f"   - Schema: {session.sql('SELECT CURRENT_SCHEMA()').collect()[0][0]}")
    
    # Cleanup
    print("\n🧹 Cleaning up...")
    session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
    session.sql(f"DROP TABLE IF EXISTS {output_table_name}").collect()
    print("✅ Cleanup complete!")
    
    session.close()
    print("🔌 Session closed.")
    """
    
    print("Demo completed! 🎉")


def show_test_comparison():
    """
    Show a comparison of the three testing approaches.
    """
    print("\n=== Testing Approaches Comparison ===\n")
    
    print("1. 🏠 Local Testing Framework (Snowpark)")
    print("   ✅ Pros:")
    print("      - No Snowflake connection required")
    print("      - Fast execution")
    print("      - No costs")
    print("      - Good for unit testing")
    print("   ❌ Cons:")
    print("      - Limited Snowflake-specific features")
    print("      - May not catch all edge cases")
    print("      - Column names converted to uppercase")
    print("      - Some functions may behave differently")
    
    print("\n2. 🎭 Local Mocking")
    print("   ✅ Pros:")
    print("      - No dependencies on Snowflake")
    print("      - Very fast execution")
    print("      - Complete control over test data")
    print("      - Good for logic testing")
    print("   ❌ Cons:")
    print("      - Doesn't test actual Snowflake integration")
    print("      - May miss Snowflake-specific issues")
    print("      - Requires maintaining mock implementations")
    
    print("\n3. ☁️ Live Snowflake Testing")
    print("   ✅ Pros:")
    print("      - Tests actual Snowflake behavior")
    print("      - Catches all Snowflake-specific issues")
    print("      - Tests performance and scalability")
    print("      - Tests data persistence and consistency")
    print("   ❌ Cons:")
    print("      - Requires Snowflake credentials")
    print("      - Slower execution")
    print("      - Incurs Snowflake costs")
    print("      - Requires network connectivity")
    
    print("\n📋 Recommendation:")
    print("   - Use Local Testing Framework for development and CI/CD")
    print("   - Use Local Mocking for unit tests and logic validation")
    print("   - Use Live Snowflake Testing for integration tests and staging")


if __name__ == "__main__":
    demo_live_snowflake_testing()
    show_test_comparison()
