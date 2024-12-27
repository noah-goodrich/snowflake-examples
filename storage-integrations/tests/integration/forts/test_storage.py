import pytest
from unittest.mock import Mock, patch
from forts.storage import StorageIntegrationFort


@pytest.fixture
def mock_snow():
    return Mock()


@pytest.fixture
def storage_fort(mock_snow):
    return StorageIntegrationFort(mock_snow, environment='dev')


class TestStorageIntegrationFort:
    """Unit tests for StorageIntegrationFort"""

    def test_create_iam_policy(self, storage_fort):
        """Test IAM policy document creation"""
        policy = storage_fort.create_iam_policy('test-bucket', 'data/')

        assert policy['Version'] == '2012-10-17'
        assert len(policy['Statement']) == 1
        assert 's3:PutObject' in policy['Statement'][0]['Action']
        assert 'arn:aws:s3:::test-bucket/data/*' in policy['Statement'][0]['Resource']

    def test_create_read_only_iam_policy(self, storage_fort):
        """Test read-only IAM policy creation"""
        policy = storage_fort.create_iam_policy('test-bucket', read_only=True)

        assert 's3:PutObject' not in policy['Statement'][0]['Action']
        assert 's3:GetObject' in policy['Statement'][0]['Action']

    def test_create_storage_integration(self, storage_fort):
        """Test storage integration creation SQL"""
        storage_fort.create_storage_integration(
            'test_integration',
            'arn:aws:iam::123456789012:role/test-role',
            ['s3://bucket1/path1/', 's3://bucket2/path2/'],
            ['s3://bucket1/sensitive/']
        )

        mock_sql = storage_fort.snow.execute.call_args[0][0]
        assert 'CREATE STORAGE INTEGRATION test_integration' in mock_sql
        assert "STORAGE_PROVIDER = 'S3'" in mock_sql
        assert 'STORAGE_AWS_ROLE_ARN' in mock_sql
        assert 'STORAGE_BLOCKED_LOCATIONS' in mock_sql

    def test_get_integration_details(self, storage_fort):
        """Test retrieval of integration details"""
        mock_result = [
            Mock(property='STORAGE_AWS_IAM_USER_ARN',
                 property_value='arn:aws:iam::123456789012:user/test'),
            Mock(property='STORAGE_AWS_EXTERNAL_ID', property_value='ABC123')
        ]
        storage_fort.snow.execute.return_value = mock_result

        details = storage_fort.get_integration_details('test_integration')

        assert details['STORAGE_AWS_IAM_USER_ARN'] == 'arn:aws:iam::123456789012:user/test'
        assert details['STORAGE_AWS_EXTERNAL_ID'] == 'ABC123'


@pytest.mark.integration
class TestStorageIntegrationFortIntegration:
    """Integration tests for StorageIntegrationFort"""

    @pytest.fixture
    def integration_fort(self):
        """Creates a real StorageIntegrationFort instance"""
        from snowflake.connector import SnowflakeConnection
        conn = SnowflakeConnection(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            role='ACCOUNTADMIN'
        )
        return StorageIntegrationFort(conn, environment='test')

    def test_end_to_end_integration_setup(self, integration_fort):
        """Test complete integration setup process"""
        # Create integration
        integration_name = f"TEST_INT_{uuid.uuid4().hex[:8]}"
        integration_fort.create_storage_integration(
            integration_name,
            os.getenv('AWS_ROLE_ARN'),
            ['s3://test-bucket/data/']
        )

        # Get details
        details = integration_fort.get_integration_details(integration_name)
        assert 'STORAGE_AWS_IAM_USER_ARN' in details
        assert 'STORAGE_AWS_EXTERNAL_ID' in details

        # Create stage
        stage_name = f"TEST_STAGE_{uuid.uuid4().hex[:8]}"
        integration_fort.create_external_stage(
            stage_name,
            integration_name,
            's3://test-bucket/data/'
        )

        # Cleanup
        integration_fort.snow.execute(f"DROP STAGE IF EXISTS {stage_name}")
        integration_fort.snow.execute(
            f"DROP INTEGRATION IF EXISTS {integration_name}")
