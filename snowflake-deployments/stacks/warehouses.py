from stacks.snow import SnowStack
from typing import List, Dict, Optional


class WarehouseStack(SnowStack):
    """
    Stack for managing Snowflake warehouses.

    Example usage:
        def deploy_custom_warehouses(self):
            # Create all sizes except XLARGE
            self.deploy_warehouses(
                db="CUSTOM",
                excluded_sizes=["XLARGE"],
                overrides={'auto_suspend': 300}
            )

            # Create specific sizes with size-specific configurations
            self.deploy_warehouses(
                db="CUSTOM2",
                allowed_sizes=["SMALL", "MEDIUM"],
                size_overrides={
                    "SMALL": {'auto_suspend': 120},
                    "MEDIUM": {'auto_suspend': 300, 'max_cluster_count': 2}
                }
            )
    """
    # Default warehouse sizes if none specified
    DEFAULT_SIZES = ['XSMALL', 'SMALL', 'MEDIUM', 'LARGE', 'XLARGE']

    def deploy(self):
        """Deploy all warehouses"""
        self.deploy_bronze_warehouses()
        self.deploy_silver_warehouses()
        self.deploy_gold_warehouses()
        self.deploy_platinum_warehouses()

    def deploy_warehouses(
        self,
        db: str,
        overrides: Optional[Dict] = None,
        size_overrides: Optional[Dict[str, Dict]] = None,
        allowed_sizes: Optional[List[str]] = None,
        excluded_sizes: Optional[List[str]] = None
    ):
        """
        Deploy warehouses for a specific tier with configurable sizes and overrides

        Args:
            db: Database/tier name (e.g., 'BRONZE', 'SILVER')
            overrides: Default overrides for all warehouses in this tier
            size_overrides: Specific overrides for individual sizes
            allowed_sizes: List of sizes to create (defaults to all sizes if None)
            excluded_sizes: List of sizes to exclude from creation
        """
        sizes = allowed_sizes or self.DEFAULT_SIZES

        # Remove excluded sizes if specified
        if excluded_sizes:
            sizes = [size for size in sizes if size not in excluded_sizes]

        # Create warehouses for each size
        for size in sizes:
            # Combine default overrides with size-specific overrides
            combined_overrides = {}
            if overrides:
                combined_overrides.update(overrides)
            if size_overrides and size in size_overrides:
                combined_overrides.update(size_overrides[size])

            self.create_or_alter_warehouse(
                db=db,
                size=size,
                overrides=combined_overrides if combined_overrides else None
            )

    def deploy_bronze_warehouses(self):
        """Deploy Bronze tier warehouses for raw data loading and processing"""
        self.deploy_warehouses(
            db="BRONZE",
            allowed_sizes=["SMALL"],  # Only create SMALL warehouse
            overrides={
                'auto_suspend': 300,
                'min_cluster_count': 1,
                'max_cluster_count': 3,
                'scaling_policy': 'ECONOMY'
            }
        )

    def deploy_silver_warehouses(self):
        """Deploy Silver tier warehouses for data transformation"""
        # Creates all sizes with default configuration
        self.deploy_warehouses(db="SILVER")

    def deploy_gold_warehouses(self):
        """Deploy Gold tier warehouses for analytics and reporting"""
        self.deploy_warehouses(
            db="GOLD",
            allowed_sizes=["XSMALL"],
            overrides={
                'auto_suspend': 120
            }
        )

    def deploy_platinum_warehouses(self):
        # Use size-specific overrides for Platinum warehouses
        self.deploy_warehouses(
            db="PLATINUM",
            size_overrides={
                'MEDIUM': {
                    'auto_suspend': 600,
                    'min_cluster_count': 1,
                    'max_cluster_count': 4,
                    'scaling_policy': 'ECONOMY'
                },
                'LARGE': {
                    'auto_suspend': 600,
                    'min_cluster_count': 1,
                    'max_cluster_count': 4,
                    'scaling_policy': 'ECONOMY'
                }
            },
            allowed_sizes=["MEDIUM", "LARGE"]
        )

        # Alternative approach using size-specific overrides:
        # self.deploy_warehouses(
        #     db="PLATINUM",
        #     overrides={
        #         'auto_suspend': 600,
        #         'min_cluster_count': 1,
        #         'max_cluster_count': 4,
        #         'scaling_policy': 'ECONOMY'
        #     },
        #     allowed_sizes=["MEDIUM", "LARGE"]
        # )
