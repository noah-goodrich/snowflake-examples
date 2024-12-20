# Warehouse Configuration

Our Snowflake implementation uses database-specific warehouses to maintain clear cost attribution and simplify resource management.

## Warehouse Naming Convention

Warehouses follow the pattern: `<ENV>_<DATABASE>_<SIZE>`

Examples:
- DEV_BRONZE_SMALL
- PROD_SILVER_MEDIUM
- DEV_GOLD_XSMALL

## Database-Specific Warehouses

### 1. Bronze Database Warehouses
Optimized for data ingestion and raw data queries.

**Configuration:**
```yaml
BRONZE_WH:
  size: SMALL
  min_cluster_count: 1
  max_cluster_count: 2
  auto_suspend: 60
  auto_resume: true
  initially_suspended: true
  enable_query_acceleration: false
  statement_timeout_seconds: 3600
```

**Use Cases:**
- Raw data loading
- Initial data validation
- Source data exploration
- ELT processes starting from raw data

### 2. Silver Database Warehouses
Configured for data transformation and standardization processes.

**Configuration:**
```yaml
SILVER_WH:
  size: MEDIUM
  min_cluster_count: 1
  max_cluster_count: 3
  auto_suspend: 300
  auto_resume: true
  initially_suspended: true
  enable_query_acceleration: true
  statement_timeout_seconds: 7200
```

**Use Cases:**
- Data standardization
- Quality validation
- Cross-source integration
- Feature engineering

### 3. Gold Database Warehouses
Optimized for business analytics and reporting.

**Configuration:**
```yaml
GOLD_WH:
  size: SMALL
  min_cluster_count: 1
  max_cluster_count: 2
  auto_suspend: 300
  auto_resume: true
  initially_suspended: true
  enable_query_acceleration: true
  statement_timeout_seconds: 1800
```

**Use Cases:**
- Business reporting
- Analytics queries
- Dashboard refreshes
- Cross-database final transformations

### 4. Platinum Database Warehouses
Configured for machine learning operations.

**Configuration:**
```yaml
PLATINUM_WH:
  size: LARGE
  min_cluster_count: 1
  max_cluster_count: 3
  auto_suspend: 600
  auto_resume: true
  initially_suspended: true
  enable_query_acceleration: true
  statement_timeout_seconds: 14400
```

**Use Cases:**
- Feature store operations
- Model training data preparation
- Prediction serving
- ML metadata operations

## Query Cost Attribution

### Using Query Tags
```sql
ALTER SESSION SET QUERY_TAG = 'ETL_LOAD_CUSTOMERS';
-- Query will be tagged for cost tracking even if using a different database's warehouse
```

### Cross-Database Operations
When performing cross-database operations:
1. Use the destination database's warehouse
2. Tag queries appropriately for cost tracking
3. Monitor warehouse utilization by query type

Example:
```sql
-- Loading from BRONZE to SILVER
USE WAREHOUSE SILVER_WH;
ALTER SESSION SET QUERY_TAG = 'BRONZE_TO_SILVER_CUSTOMER_TRANSFORM';
INSERT INTO SILVER.CLEAN.CUSTOMERS
SELECT /* ... */
FROM BRONZE.RAW.CUSTOMERS;
```

## Size Guidelines

| Size    | Credits/Hour | Recommended Use Case |
|---------|-------------|---------------------|
| X-Small | 1          | Development, Testing |
| Small   | 2          | Light Production Loads |
| Medium  | 4          | Standard Production |
| Large   | 8          | Heavy Processing |
| X-Large | 16         | Special Workloads |

## Configuration Options

### Core Settings
```yaml
warehouse_name:
  size: XSMALL|SMALL|MEDIUM|LARGE|XLARGE
  auto_suspend: 60  # seconds
  auto_resume: true|false
  initially_suspended: true|false
```

### Advanced Settings
```yaml
warehouse_name:
  min_cluster_count: 1
  max_cluster_count: 3
  scaling_policy: STANDARD|ECONOMY
  enable_query_acceleration: true|false
  statement_timeout_seconds: 3600
```

## Resource Monitoring

### Credit Quotas
```yaml
resource_monitors:
  DEV_MONITOR:
    credit_quota: 100
    frequency: MONTHLY
    start_timestamp: 2024-01-01
    notify_triggers:
      - threshold: 80
        notification_level: WARNING
      - threshold: 90
        notification_level: ERROR
      - threshold: 100
        notification_level: ERROR
        suspend: true
```

### Warehouse Assignment
```yaml
warehouses:
  DEV_LOAD_SMALL:
    resource_monitor: DEV_MONITOR
  DEV_TRANSFORM_MEDIUM:
    resource_monitor: DEV_MONITOR
```

## Best Practices

1. **Sizing**
   - Start small and scale up as needed
   - Monitor query performance
   - Adjust based on workload patterns

2. **Auto-suspension**
   - Enable for all warehouses
   - Set appropriate timeout values
   - Monitor idle time

3. **Multi-clustering**
   - Enable for concurrent workloads
   - Set appropriate limits
   - Monitor cluster usage

4. **Resource Monitoring**
   - Set appropriate credit limits
   - Configure notifications
   - Review usage patterns

5. **Query Optimization**
   - Monitor long-running queries
   - Implement timeout policies
   - Use query tags for tracking

## Configuration

Warehouse configuration is managed in `stacks/snowflake/config/warehouses.yaml`. See the [warehouses.yaml](../../stacks/snowflake/config/warehouses.yaml) file for the complete configuration.
