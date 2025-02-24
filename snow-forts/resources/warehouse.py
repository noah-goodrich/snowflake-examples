from dataclasses import dataclass
from typing import Optional

from snowflake.core import Root
from snowflake.core._common import CreateMode
from snowflake.core.warehouse import Warehouse as SnowflakeWarehouse


@dataclass
class WarehouseConfig:
    """Configuration for warehouse operations"""
    name: str
    size: str = "XSMALL"
    auto_suspend: int = 60
    auto_resume: bool = True
    min_cluster_count: int = 1
    max_cluster_count: int = 1
    initially_suspended: bool = True
    prefix_with_environment: bool = True

    def validate(self) -> None:
        """Validates warehouse configuration"""
        valid_sizes = {"XSMALL", "SMALL", "MEDIUM", "LARGE", "XLARGE"}
        if self.size.upper() not in valid_sizes:
            raise ValueError(
                f"Invalid warehouse size. Must be one of {valid_sizes}")
        if self.auto_suspend < 0:
            raise ValueError("auto_suspend must be >= 0")
        if self.min_cluster_count < 1:
            raise ValueError("min_cluster_count must be >= 1")
        if self.max_cluster_count < self.min_cluster_count:
            raise ValueError("max_cluster_count must be >= min_cluster_count")


class Warehouse:
    """Manages Snowflake warehouse operations"""

    def __init__(self, snow: Root, environment: str):
        self.snow = snow
        self.environment = environment

    def create(self, config: WarehouseConfig, mode: CreateMode = CreateMode.if_not_exists) -> SnowflakeWarehouse:
        """Creates a new warehouse"""
        config.validate()
        name = self._format_name(config.name, config.prefix_with_environment)

        # Create warehouse
        return self.snow.warehouses.create(
            SnowflakeWarehouse(
                name=name,
                warehouse_size=config.size,
                auto_suspend=config.auto_suspend,
                auto_resume=str(config.auto_resume).lower(),
                min_cluster_count=config.min_cluster_count,
                max_cluster_count=config.max_cluster_count,
                initially_suspended=str(config.initially_suspended).lower()
            ),
            mode=mode
        )

    def alter(self, name: str, config: WarehouseConfig) -> SnowflakeWarehouse:
        """Alters an existing warehouse"""
        config.validate()
        warehouse = self.get(name)
        if not warehouse:
            raise ValueError(f"Warehouse {name} does not exist")

        # Update warehouse properties
        return self.create(
            config,
            mode=CreateMode.or_replace
        )

    def drop(self, name: str, cascade: bool = True) -> None:
        """Drops a warehouse"""
        warehouse = self.get(name)
        if warehouse:
            warehouse.drop(cascade=cascade)

    def get(self, name: str) -> Optional[SnowflakeWarehouse]:
        """Gets a warehouse by name"""
        try:
            return self.snow.warehouses[self._format_name(name, True)]
        except KeyError:
            try:
                return self.snow.warehouses[name.upper()]
            except KeyError:
                return None

    def suspend(self, name: str) -> None:
        """Suspends a warehouse"""
        warehouse = self.get(name)
        if warehouse:
            self.snow.session.sql(
                f"ALTER WAREHOUSE {warehouse.name} SUSPEND").collect()

    def resume(self, name: str) -> None:
        """Resumes a warehouse"""
        warehouse = self.get(name)
        if warehouse:
            self.snow.session.sql(
                f"ALTER WAREHOUSE {warehouse.name} RESUME").collect()

    def _format_name(self, name: str, use_environment: bool) -> str:
        """Formats warehouse name according to conventions"""
        if use_environment:
            return f"{self.environment}_{name}".upper()
        return name.upper()
