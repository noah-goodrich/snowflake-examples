# Storage Integration Configuration

Our Snowflake implementation uses storage integrations to securely connect with external storage systems (primarily AWS S3) for data loading and unloading.

## Setup Process

1. Create Snowflake Storage Integration
2. Retrieve Integration Details from Snowflake
3. [Set up IAM Role and Policies](../iam/README.md#storage-integration-roles)
4. Update Storage Integration with IAM Role ARN

## Step-by-Step Configuration

### 1. Create Storage Integration
```sql
-- This will be handled by our CDK stack
CREATE STORAGE INTEGRATION RAW_DATA
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'placeholder'  -- Will be updated after IAM setup
  STORAGE_ALLOWED_LOCATIONS = ('s3://raw-data-bucket/landing/', 's3://raw-data-bucket/archive/')
  STORAGE_BLOCKED_LOCATIONS = ('s3://raw-data-bucket/sensitive/');
```

### 2. Retrieve Integration Details
After creating the integration, retrieve the necessary information for IAM setup:
```sql
DESC STORAGE INTEGRATION RAW_DATA;
```
Note the following values:
- STORAGE_AWS_IAM_USER_ARN
- STORAGE_AWS_EXTERNAL_ID

### 3. IAM Configuration
Follow the [IAM Setup Guide](../iam/README.md#storage-integration-roles) to:
1. Create the IAM role
2. Configure trust relationships
3. Attach necessary policies

### 4. Update Integration
After IAM setup, update the storage integration with the role ARN:
```sql
ALTER STORAGE INTEGRATION RAW_DATA
  SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeLoadRole';
```

**Configuration:**
```yaml
RAW_DATA:
  s3_bucket: "raw-data-bucket"
  aws_role: "SnowflakeLoadRole"
  enabled: true
  storage_allowed_locations:
    - "s3://raw-data-bucket/landing/"
    - "s3://raw-data-bucket/archive/"
  storage_blocked_locations:
    - "s3://raw-data-bucket/sensitive/"
```

**IAM Role Policy:**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion"
            ],
            "Resource": [
                "arn:aws:s3:::raw-data-bucket/landing/*",
                "arn:aws:s3:::raw-data-bucket/archive/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::raw-data-bucket",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "landing/*",
                        "archive/*"
                    ]
                }
            }
        }
    ]
}
```

### 2. Export Integration
Used for data exports from GOLD database.

**Configuration:**
```yaml
EXPORT_DATA:
  s3_bucket: "export-data-bucket"
  aws_role: "SnowflakeExportRole"
  enabled: true
  storage_allowed_locations:
    - "s3://export-data-bucket/outbound/"
  storage_blocked_locations:
    - "s3://export-data-bucket/internal/"
```

## AWS IAM Setup

### 1. Create IAM Role
```yaml
iam_roles:
  SnowflakeLoadRole:
    trust_relationship:
      Service: s3.amazonaws.com
      AWS: "arn:aws:iam::${AWS_ACCOUNT}:user/snowflake"
    policies:
      - S3ReadOnly
      - SnowflakeIntegrationPolicy

  SnowflakeExportRole:
    trust_relationship:
      Service: s3.amazonaws.com
      AWS: "arn:aws:iam::${AWS_ACCOUNT}:user/snowflake"
    policies:
      - S3WriteOnly
      - SnowflakeIntegrationPolicy
```

### 2. Configure Trust Relationship
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::${SNOWFLAKE_ACCOUNT}:user/snowflake"
            },
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": "${EXTERNAL_ID}"
                }
            }
        }
    ]
}
```

## Configuration Management

Storage integrations are managed in `stacks/snowflake/config/storage.yaml`:

```yaml
storage_integrations:
  RAW_DATA:
    s3_bucket: "raw-data-bucket"
    aws_role: "SnowflakeLoadRole"
    enabled: true
    storage_allowed_locations:
      - "s3://raw-data-bucket/landing/"
      - "s3://raw-data-bucket/archive/"
    storage_blocked_locations:
      - "s3://raw-data-bucket/sensitive/"
    comment: "Integration for raw data ingestion"

  EXPORT_DATA:
    s3_bucket: "export-data-bucket"
    aws_role: "SnowflakeExportRole"
    enabled: true
    storage_allowed_locations:
      - "s3://export-data-bucket/outbound/"
    storage_blocked_locations:
      - "s3://export-data-bucket/internal/"
    comment: "Integration for data exports"
```

## Security Considerations

1. **Access Control**
   - Use separate roles for read and write operations
   - Implement least privilege access
   - Regular rotation of credentials
   - Monitor access patterns

2. **Data Protection**
   - Enable bucket encryption
   - Use blocked locations for sensitive data
   - Implement object lifecycle policies
   - Regular security audits

3. **Network Security**
   - VPC endpoint considerations
   - IP range restrictions
   - SSL/TLS enforcement
   - Network path monitoring

## Best Practices

1. **Integration Setup**
   - One integration per data flow type
   - Clear naming conventions
   - Documented purpose and ownership
   - Regular testing and validation

2. **S3 Configuration**
   - Organized bucket structure
   - Clear path conventions
   - Appropriate retention policies
   - Cost optimization strategies

3. **Monitoring**
   - Integration status checks
   - Usage patterns
   - Error tracking
   - Cost monitoring

4. **Maintenance**
   - Regular permission review
   - Cleanup of unused integrations
   - Documentation updates
   - Configuration version control
