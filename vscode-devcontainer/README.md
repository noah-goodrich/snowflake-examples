# LocalStack Development Environment Setup

This guide will help you set up a local development environment using LocalStack for AWS + Snowflake service emulation.

## Prerequisites

Before starting, ensure you have the following installed:
- VSCode or Cursor IDE
- Docker CLI or Docker Desktop

## Setup Instructions

### 1. Start Docker
Ensure Docker is running on your system.

### 2. Configure AWS CLI Credentials

Create or modify the following AWS configuration files:

In `~/.aws/config`:
```
[localstack]
region = us-east-1
output = json
```

In `~/.aws/credentials`:

```
[localstack]
aws_access_key_id = test
aws_secret_access_key = test
```

### 3. (Optional) Configure Local DNS

Add the following line to your `/etc/hosts` file:

127.0.0.1 localhost.localstack.cloud

This step can help prevent DNS resolution issues with some LocalStack services.

### 4. Start LocalStack

Run the following command to start LocalStack:

```
docker run --rm -it -p 4566:4566 -p 4510-4559:4510-4559 localstack/localstack
```

### 5. Verify Setup

Once LocalStack is running, you can verify your setup with the following commands:

1. Create a test S3 bucket:
```
aws s3 mb s3://test-bucket
```

2. List S3 buckets to verify creation:
aws s3 ls

3. Test Snowflake connectivity:
curl -d '{}' snowflake.localhost.localstack.cloud:4566/session

You should receive the response: {"success": true}

## Troubleshooting

If you encounter any issues:
1. Ensure Docker is running
2. Verify LocalStack container is running with `docker ps`
3. Check AWS credentials are properly configured
4. Ensure ports 4566 and 4510-4559 are not in use by other applications
5. Verify your /etc/hosts entry if you're having DNS resolution issues

## Next Steps

You now have a working LocalStack environment! You can proceed with developing and testing your AWS applications locally.
