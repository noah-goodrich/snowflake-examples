from typing import List, Optional, Dict, Any
from .fort import SnowFort


class StorageIntegrationFort(SnowFort):
    """Manages Snowflake storage integrations for secure access to cloud storage.

    This class implements the storage integration setup process for AWS S3, handling:
    - Storage integration creation and configuration
    - IAM role and policy management
    - External stage creation with proper permissions

    The implementation follows Snowflake's recommended security practices for
    delegating authentication responsibility to Snowflake IAM entities.

    Attributes:
        REQUIRED_S3_PERMISSIONS (List[str]): Basic S3 permissions needed for read access
        ADDITIONAL_S3_PERMISSIONS (Dict[str, str]): Optional permissions for specific operations
    """

    REQUIRED_S3_PERMISSIONS = [
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket"
    ]

    ADDITIONAL_S3_PERMISSIONS = {
        "s3:PutObject": "Unload files to bucket",
        "s3:DeleteObject": "Purge files after load or manual removal"
    }

    def create_iam_policy(self, bucket_name: str, prefix: str = "*", read_only: bool = False) -> Dict[str, Any]:
        """Creates an IAM policy document for S3 bucket access.

        Args:
            bucket_name (str): Name of the S3 bucket
            prefix (str, optional): Path prefix to restrict access. Defaults to "*".
            read_only (bool, optional): If True, creates read-only policy. Defaults to False.

        Returns:
            Dict[str, Any]: IAM policy document in JSON format
        """
        permissions = self.REQUIRED_S3_PERMISSIONS.copy()
        if not read_only:
            permissions.extend(self.ADDITIONAL_S3_PERMISSIONS.keys())

        return {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": permissions,
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}/{prefix}/*",
                        f"arn:aws:s3:::{bucket_name}"
                    ]
                }
            ]
        }

    def create_storage_integration(
        self,
        integration_name: str,
        role_arn: str,
        allowed_locations: List[str],
        blocked_locations: Optional[List[str]] = None,
        external_id: Optional[str] = None
    ) -> 'StorageIntegrationFort':
        """Creates a Snowflake storage integration for AWS S3.

        Args:
            integration_name (str): Name for the storage integration
            role_arn (str): AWS IAM role ARN to be assumed
            allowed_locations (List[str]): List of allowed S3 locations (e.g., ['s3://bucket/path/'])
            blocked_locations (Optional[List[str]], optional): List of blocked S3 locations. Defaults to None.
            external_id (Optional[str], optional): Custom external ID for cross-account access. Defaults to None.

        Returns:
            StorageIntegrationFort: The current instance for method chaining

        Example:
            ```python
            fort.create_storage_integration(
                'my_s3_int',
                'arn:aws:iam::001234567890:role/myrole',
                ['s3://mybucket/data/'],
                ['s3://mybucket/sensitive/']
            )
            ```
        """
        create_sql = f"""
        CREATE STORAGE INTEGRATION {integration_name}
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            ENABLED = TRUE
            STORAGE_AWS_ROLE_ARN = '{role_arn}'
            STORAGE_ALLOWED_LOCATIONS = ({','.join(f"'{loc}'" for loc in allowed_locations)})
        """

        if blocked_locations:
            create_sql += f"STORAGE_BLOCKED_LOCATIONS = ({','.join(f"'{loc}'" for loc in blocked_locations)})"

        if external_id:
            create_sql += f"\nSTORAGE_AWS_EXTERNAL_ID = '{external_id}'"

        self.snow.execute(create_sql)
        return self

    def get_integration_details(self, integration_name: str) -> Dict[str, str]:
        """Retrieves the AWS IAM user details for a storage integration.

        Args:
            integration_name (str): Name of the storage integration

        Returns:
            Dict[str, str]: Dictionary containing IAM user ARN and external ID

        Example:
            ```python
            details = fort.get_integration_details('my_s3_int')
            iam_user_arn = details['STORAGE_AWS_IAM_USER_ARN']
            external_id = details['STORAGE_AWS_EXTERNAL_ID']
            ```
        """
        result = self.snow.execute(f"DESC INTEGRATION {integration_name}")
        details = {}
        for row in result:
            if row.property in ['STORAGE_AWS_IAM_USER_ARN', 'STORAGE_AWS_EXTERNAL_ID']:
                details[row.property] = row.property_value
        return details

    def create_external_stage(
        self,
        stage_name: str,
        integration_name: str,
        url: str,
        file_format: Optional[str] = None,
        schema: str = 'public'
    ) -> 'StorageIntegrationFort':
        """Creates an external stage using a storage integration.

        Args:
            stage_name (str): Name for the external stage
            integration_name (str): Name of the storage integration to use
            url (str): S3 URL for the stage (e.g., 's3://bucket/path/')
            file_format (Optional[str], optional): Name of file format to use. Defaults to None.
            schema (str, optional): Schema for stage creation. Defaults to 'public'.

        Returns:
            StorageIntegrationFort: The current instance for method chaining

        Example:
            ```python
            fort.create_external_stage(
                'my_s3_stage',
                'my_s3_int',
                's3://mybucket/data/',
                'my_csv_format'
            )
            ```
        """
        create_sql = f"""
        CREATE STAGE {schema}.{stage_name}
            STORAGE_INTEGRATION = {integration_name}
            URL = '{url}'
        """

        if file_format:
            create_sql += f"FILE_FORMAT = {file_format}"

        self.snow.execute(create_sql)
        return self
