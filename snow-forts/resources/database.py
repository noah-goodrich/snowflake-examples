from dataclasses import dataclass
from typing import Optional, List

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.database import Database as SnowflakeDatabase
from snowflake.core.schema import Schema


@dataclass
class DatabaseConfig:
    """Configuration for database operations"""
    name: str
    schemas: Optional[List[str]] = None
    comment: Optional[str] = None
    prefix_with_environment: bool = True

    def validate(self) -> None:
        """Validates database configuration"""
        if not self.name:
            raise ValueError("Database name cannot be empty")
        if self.schemas and not all(self.schemas):
            raise ValueError("Schema names cannot be empty")


class Database:
    """Manages Snowflake database operations"""

    def __init__(self, snow: Root, environment: str):
        self.snow = snow
        self.environment = environment

    def create(self, config: DatabaseConfig, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeDatabase:
        """Creates a new database with optional schemas"""
        config.validate()
        name = self._format_name(config.name, config.prefix_with_environment)

        # Create database
        database = self.snow.databases.create(
            SnowflakeDatabase(
                name=name,
                comment=config.comment
            ),
            mode=mode
        )

        # Create schemas if specified
        if config.schemas:
            for schema_name in config.schemas:
                database.schemas.create(
                    Schema(name=schema_name),
                    mode=CreateMode.if_not_exists
                )

        return database

    def alter(self, name: str, config: DatabaseConfig) -> SnowflakeDatabase:
        """Alters an existing database"""
        config.validate()
        database = self.get(name)
        if not database:
            raise ValueError(f"Database {name} does not exist")

        # Update database properties
        return self.create(config, mode=CreateMode.or_replace)

    def drop(self, name: str, cascade: bool = True) -> None:
        """Drops a database"""
        database = self.get(name)
        if database:
            database.drop(cascade=cascade)

    def get(self, name: str) -> Optional[SnowflakeDatabase]:
        """Gets a database by name"""
        try:
            return self.snow.databases[self._format_name(name, True)]
        except KeyError:
            try:
                return self.snow.databases[name.upper()]
            except KeyError:
                return None

    def create_schema(self, database_name: str, schema_name: str) -> Schema:
        """Creates a new schema in the specified database"""
        database = self.get(database_name)
        if not database:
            raise ValueError(f"Database {database_name} does not exist")

        return database.schemas.create(
            Schema(name=schema_name),
            mode=CreateMode.if_not_exists
        )

    def drop_schema(self, database_name: str, schema_name: str, cascade: bool = True) -> None:
        """Drops a schema from the specified database"""
        database = self.get(database_name)
        if database:
            try:
                schema = database.schemas[schema_name.upper()]
                schema.drop(cascade=cascade)
            except KeyError:
                pass

    def _format_name(self, name: str, use_environment: bool) -> str:
        """Formats database name according to conventions"""
        if use_environment:
            return f"{self.environment}_{name}".upper()
        return name.upper()
