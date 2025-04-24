import pytest
import logging
from snowflake.snowpark import Session
from snowflake.core import Root
import boto3
import json

from tests.utils import e2e_only, e2e_assert

logger = logging.getLogger(__name__)


def get_snowflake_session(secret_name: str) -> Session:
    """Create Snowflake session from AWS Secrets Manager credentials"""
    session = boto3.session.Session()
    client = session.client('secretsmanager')

    try:
        secret = client.get_secret_value(SecretId=secret_name)
        creds = json.loads(secret['SecretString'])

        return Session.builder.configs({
            "account": creds['account'],
            "host": creds['host'],
            "user": creds['username'],
            "private_key": creds['private_key'],
            "role": creds['role'],
            "warehouse": "COMPUTE_WH"  # Default warehouse
        }).create()
    except Exception as e:
        logger.error(f"Failed to get Snowflake credentials: {str(e)}")
        raise


@pytest.fixture(scope="module")
def snow():
    """Create a Snowflake session for e2e tests"""
    session = get_snowflake_session('snowflake/accountadmin')
    return Root(session)


@e2e_only
def test_complete_deployment(snow):
    """Test that the complete deployment creates all expected resources"""
    from deploy import main
    import sys

    # Run the deploy script
    sys.argv = ['deploy.py', '--env', 'dev', '--fort', 'all']
    assert main() == 0, "Deployment failed"

    # Verify admin resources
    e2e_assert(
        snow.warehouses['DEV_ADMIN_WH'] is not None,
        "Admin warehouse not created"
    )
    e2e_assert(
        snow.databases['DEV_ADMIN_DB'] is not None,
        "Admin database not created"
    )
    e2e_assert(
        snow.roles['DEV_ADMIN_ROLE'] is not None,
        "Admin role not created"
    )
    e2e_assert(
        snow.users['DEV_ADMIN_USER'] is not None,
        "Admin user not created"
    )

    # Verify medallion resources
    for layer in ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']:
        e2e_assert(
            snow.databases[f'DEV_{layer}_DB'] is not None,
            f"{layer} database not created"
        )
        e2e_assert(
            snow.warehouses[f'DEV_{layer}_WH'] is not None,
            f"{layer} warehouse not created"
        )
        e2e_assert(
            snow.roles[f'DEV_{layer}_ROLE'] is not None,
            f"{layer} role not created"
        )

    # Verify warehouse configurations
    admin_wh = snow.warehouses['DEV_ADMIN_WH']
    e2e_assert(
        admin_wh.warehouse_size == 'XSMALL',
        "Admin warehouse size incorrect"
    )
    e2e_assert(
        admin_wh.auto_suspend == 60,
        "Admin warehouse auto-suspend incorrect"
    )

    platinum_wh = snow.warehouses['DEV_PLATINUM_WH']
    e2e_assert(
        platinum_wh.warehouse_size == 'XLARGE',
        "Platinum warehouse size incorrect"
    )
    e2e_assert(
        platinum_wh.enable_query_acceleration,
        "Platinum warehouse query acceleration not enabled"
    )
    e2e_assert(
        platinum_wh.query_acceleration_max_scale_factor == 8,
        "Platinum warehouse query acceleration scale factor incorrect"
    )

    # Verify role hierarchy
    admin_role = snow.roles['DEV_ADMIN_ROLE']
    e2e_assert(
        'DEV_BRONZE_ROLE' in admin_role.granted_roles,
        "Admin role not granted to Bronze role"
    )
    e2e_assert(
        'DEV_SILVER_ROLE' in admin_role.granted_roles,
        "Admin role not granted to Silver role"
    )
    e2e_assert(
        'DEV_GOLD_ROLE' in admin_role.granted_roles,
        "Admin role not granted to Gold role"
    )
    e2e_assert(
        'DEV_PLATINUM_ROLE' in admin_role.granted_roles,
        "Admin role not granted to Platinum role"
    )

    # Verify database privileges
    for layer in ['BRONZE', 'SILVER', 'GOLD', 'PLATINUM']:
        db = snow.databases[f'DEV_{layer}_DB']
        role = snow.roles[f'DEV_{layer}_ROLE']
        e2e_assert(
            role in db.granted_roles,
            f"{layer} role not granted to {layer} database"
        )
