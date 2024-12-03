from aws_cdk import Stack
from constructs import Construct
from typing import Any, Dict
import yaml
import boto3
from botocore.exceptions import ClientError
from snowflake.core import Root
from snowflake.core.database import Database
from snowflake.core.role import Role
from snowflake.core.user import User
from snowflake.core.warehouse import Warehouse


class SnowStack(Stack):
    def __init__(self, scope: Construct, id: str, snow: Root, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Initialize properties
        self.snow = snow
        self.configs = {
            'environments': self.load_config('stacks/config/environments.yaml'),
            'warehouses': self.load_config('stacks/config/warehouses.yaml'),
            'roles': self.load_config('stacks/config/roles.yaml'),
            'users': self.load_config('stacks/config/users.yaml'),
            'databases': self.load_config('stacks/config/databases.yaml')
        }
        self.warehouse_defaults = None
        self.warehouses = None

    def load_config(self, path: str) -> dict:
        """Load and parse a YAML configuration file.

        Args:
            path: Path to the YAML configuration file

        Returns:
            dict: Parsed YAML configuration

        Raises:
            FileNotFoundError: If configuration file does not exist
            yaml.YAMLError: If YAML parsing fails
        """
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def get_secret(self, secret_name: str) -> Dict[str, Any]:
        """Get secret from AWS Secrets Manager"""
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager')
        try:
            return client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            raise Exception(f"Failed to get secret {secret_name}: {e}")

    def database(self, db: str, config: dict) -> 'SnowStack':
        """Create or alter a database with the given configuration.
        Args:
            db: Database name
            config: Database configuration
        """
        self.snow.databases[db].create_or_alter(Database(
            name=db,
            comment=config['description']
        ))

    def warehouse(self, db: str, size: str | dict, environment: str | None = None) -> 'SnowStack':
        """Create or alter a warehouse with the given configuration.
        Args:
            db: Database name
            size: Warehouse size
            environment: Environment name (dev, stg, prod, etc.)
        """
        if self.warehouses is None:
            self.warehouses = {}

        defaults = self.configs['warehouses']['defaults']

        if type(size) == dict:
            overrides = list(size.values())[0]
            size = list(size.keys())[0]
        else:
            overrides = {}

        if environment is not None:
            name = f"{environment.lower()}_{db.lower()}_{size.lower()}"
        else:
            name = f"{db.lower()}_{size.lower()}"

        # Build warehouse properties
        properties = {
            'warehouse_size': size,
            'auto_suspend': overrides.get('auto_suspend', defaults['auto_suspend']),
            'auto_resume': overrides.get('auto_resume', defaults['auto_resume']),
            'initially_suspended': overrides.get('initially_suspended', defaults['initially_suspended']),
            'min_cluster_count': overrides.get('min_cluster_count', defaults['min_cluster_count']),
            'max_cluster_count': overrides.get('max_cluster_count', defaults['max_cluster_count']),
            'scaling_policy': overrides.get('scaling_policy', defaults['scaling_policy']),
            'comment': overrides.get('comment', f'Warehouse for {environment} environment')
        }

        # Create or alter warehouse
        self.snow.warehouses[name].create_or_alter(Warehouse(
            name=name,
            warehouse_size=properties['warehouse_size'],
            auto_suspend=properties['auto_suspend'],
            auto_resume=properties['auto_resume'],
            initially_suspended=properties['initially_suspended'],
            min_cluster_count=properties['min_cluster_count'],
            max_cluster_count=properties['max_cluster_count'],
            scaling_policy=properties['scaling_policy'],
            comment=properties['comment']
        ))

        # Store reference to created warehouse
        self.warehouses[name] = {
            'name': name,
            'environment': environment,
            **properties
        }

        return self

    def role(self, role: str, config: dict) -> 'SnowStack':
        """Create a role with the given configuration"""
        pass

    def user(self, user: str, config: dict) -> 'SnowStack':
        """Create a user with the given configuration"""
        pass

    def deploy(self):
        """Deploy all resources defined in the stack"""
        pass
