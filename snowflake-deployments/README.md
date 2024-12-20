# Snowflake Data Platform Foundation

A scalable data platform implementation using Snowflake, with infrastructure managed through Python code. This project follows a "fort" pattern where each major component is encapsulated in its own fort class with specific responsibilities.

## Directory Structure

    snowflake-foundation/
    ├── deploy.py                # Main deployment script
    ├── requirements.txt         # Python dependencies
    ├── forts/                  # Fort implementations
    │   ├── __init__.py
    │   ├── fort.py            # Base fort class
    │   ├── admin.py           # Administrative components
    │   └── medallion.py       # Data platform components
    ├── libs/                   # Shared libraries
    │   ├── __init__.py
    │   └── crypt.py           # Cryptography utilities
    └── tests/                 # Test suite
        ├── integration/       # Integration tests
        │   └── forts/        # Fort-specific tests
        └── unit/             # Unit tests
            └── libs/         # Library unit tests

## Core Components

### 1. Base Fort (SnowFort)
The base fort class provides fundamental Snowflake operations including:
- Database creation and management
- Warehouse provisioning
- Role and privilege management
- AWS Secrets integration

### 2. Administrative Fort (AdminFort)
The AdminFort handles core platform administration:
- HOID administrative role creation
- Service account (SVC_HOID) setup with RSA authentication
- Administrative warehouse provisioning
- COSMERE database management

### 3. Medallion Fort (MedallionFort)
Implements the medallion architecture with four core layers:

    +-------------+  +-------------+  +-------------+  +-------------+
    |   BRONZE    |  |   SILVER   |  |    GOLD    |  |  PLATINUM   |
    |-------------|  |-------------|  |-------------|  |-------------|
    | Raw Data    |  | Cleansed &  |  | Business   |  |  ML-Ready   |
    | Landing     |  | Standard-   |  | Ready      |  |  Features   |
    | Zone        |  | ized Data   |  | Analytics  |  |  & Models   |
    +-------------+  +-------------+  +-------------+  +-------------+

## Role Hierarchy & Access Flow

                      +----------------+
                      |  ACCOUNTADMIN  |
                      +----------------+
                             |
                      +----------------+
                      |  HOID (Admin)  |
                      +----------------+
                             |
                      +----------------+
                      | Database Roles |
                      +----------------+
                      | BRONZE_RO      |
                      | BRONZE_RW      |-----------------+------------------+
                      | SILVER_RO      |                 |                  |
                      | SILVER_RW      |        +----------------+  +----------------+
                      | GOLD_RO        |        |  Service Roles |  |   Function    |
                      | GOLD_RW        |        +----------------+  |    Roles      |
                      | PLATINUM_RO    |        | SVC_AIRFLOW   |  +----------------+
                      | PLATINUM_RW    |        | SVC_FIVETRAN  |  | ML_ENGINEER     |
                      +----------------+        | SVC_HOID      |  |  DATA_ENGINEER  |
                                               +----------------+  |  DATA_ANALYST   |
                                                                   +-----------------+

## Getting Started

### 1. Environment Setup

    # Create virtual environment
    python -m venv .venv

    # Activate virtual environment
    source .venv/bin/activate  # Unix
    .venv\Scripts\activate.bat # Windows

    # Install dependencies
    pip install -r requirements.txt

### 2. AWS Configuration

    # Configure AWS credentials
    aws configure

    # Create initial secret for ACCOUNTADMIN
    aws secretsmanager create-secret \
        --name snowflake/accountadmin \
        --secret-string '{
            "account": "your-account",
            "username": "your-username",
            "private_key": "your-private-key",
            "host": "your-host",
            "role": "ACCOUNTADMIN"
        }'

### 3. Initial Deployment

    # Deploy admin infrastructure
    python deploy.py --env dev --fort admin

    # Deploy medallion architecture
    python deploy.py --env dev --fort medallion

    # Or deploy everything at once
    python deploy.py --env dev --fort all

## Cryptography Utilities

The `Crypt` class provides secure key management:
- RSA key pair generation (2048-bit)
- PKCS#8 format for private keys
- PEM encoding for storage
- Key loading and verification

## Warehouse Configurations

Each database tier has specific warehouse configurations:

    Size     Credits/Hour    Use Case
    ------------------------------------
    XSMALL   1              Development, light queries
    SMALL    2              Testing, medium workloads
    MEDIUM   4              Production, regular analytics
    LARGE    8              Heavy transformations
    XLARGE   16             Large-scale processing
    XXLARGE  32             Machine learning training
    XXXLARGE 64             Intensive operations

## Testing

Run tests using pytest:

    # Run all tests
    pytest

    # Run specific test suite
    pytest tests/integration/forts/test_admin.py
    pytest tests/unit/libs/test_crypt.py

## Best Practices

### 1. Role Management
- Never grant access roles directly to users
- Use functional roles for user access
- Follow principle of least privilege
- Regular audit of role memberships

### 2. Service Accounts
- Use RSA key authentication
- Regular key rotation (90 days)
- Minimal required privileges
- Detailed audit logging

### 3. Warehouse Usage
- Match warehouse size to workload
- Enable auto-suspend for cost control
- Use multi-cluster where appropriate
- Monitor credit consumption