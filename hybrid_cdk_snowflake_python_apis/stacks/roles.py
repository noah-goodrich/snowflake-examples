from stacks.snow import SnowStack
from typing import List, Dict, Optional


class RoleStack(SnowStack):
    """
    Stack for managing Snowflake roles and their permissions.
    Access roles are created per database following the pattern: <ENV>_<DATABASE>_<ROLE>
    Example: DEV_BRONZE_RO, PRD_SILVER_RW, etc.

    Example usage:
        def deploy_custom_role(self):
            # Create a custom role with specific database access
            self.deploy_functional_role(
                name="DATA_SCIENTIST",
                description="Data Scientist access pattern",
                database_access={
                    "BRONZE": {
                        "schemas": ["SALESFORCE", "STRIPE"],
                        "level": "RO"
                    },
                    "GOLD": {
                        "schemas": ["ANALYTICS"],
                        "level": "RW"
                    }
                },
                warehouses=["ANALYTICS_WH"],
                grants_to=["ML_ENGINEER"]
            )
    """

    def deploy(self):
        """Deploy all roles"""
        # First deploy access roles for each database
        # Then deploy other roles that depend on access roles
        self.deploy_functional_roles()
        self.deploy_service_roles()

    def deploy_functional_roles(self):
        """Deploy functional roles for different job functions"""
        # Deploy ML Engineer role
        self.deploy_functional_role(
            name=f"{self.env}_ML_ENGINEER",
            description="Machine Learning Engineer access pattern",
            database_access={
                "BRONZE": {
                    "schemas": ["SALESFORCE", "STRIPE", "HUBSPOT"],
                    "level": "RO"
                },
                "SILVER": {
                    "schemas": ["CUSTOMERS", "PRODUCTS"],
                    "level": "RW"
                },
                "GOLD": {
                    "schemas": ["ANALYTICS", "REPORTING"],
                    "level": "RO"
                },
                "PLATINUM": {
                    "schemas": ["FEATURE_STORE", "MODEL_REGISTRY"],
                    "level": "RW"
                }
            },
            warehouses=["ML_WH"]
        )

        # Similar pattern for other functional roles...

    def deploy_functional_role(
        self,
        name: str,
        description: str,
        database_access: Dict[str, Dict],
        warehouses: List[str],
        grants_to: Optional[List[str]] = None
    ):
        """
        Deploy a functional role with specific database and warehouse access.
        Grants appropriate database-specific access roles based on the access level.
        """
        self.create_or_alter_role(
            name=name,
            description=description
        )

        # Grant database-specific access roles
        for db, access in database_access.items():
            access_role = f"{self.env}_{db}_{access['level']}"
            self.grant_role_to_role(access_role, name)

        # Grant warehouse access and other roles
        for warehouse in warehouses:
            self.grant_warehouse_to_role(warehouse, name)

        if grants_to:
            for role in grants_to:
                self.grant_role_to_role(role, name)

    def deploy_service_roles(self):
        """Deploy service account roles"""
        # Deploy Fivetran role
        self.deploy_service_role(
            name="FIVETRAN",
            description="Fivetran EL service account",
            database_access={"BRONZE": "RW"},
            warehouses=["LOAD_WH"],
            warehouse_size="XSMALL"
        )

        # Deploy Airflow role
        self.deploy_service_role(
            name="AIRFLOW",
            description="Airflow orchestration service account",
            database_access={
                "BRONZE": "RW",
                "SILVER": "RW",
                "GOLD": "RW"
            },
            warehouses=["ETL_WH"],
            warehouse_size="XSMALL"
        )

    def deploy_service_role(
        self,
        name: str,
        description: str,
        database_access: Dict[str, str],
        warehouses: List[str],
        warehouse_size: str = "XSMALL",
        system_privileges: Optional[List[str]] = None
    ):
        """
        Deploy a service role with specific access patterns

        Args:
            name: Role name
            description: Role description
            database_access: Dictionary mapping databases to access levels
            warehouses: List of warehouses to grant access to
            warehouse_size: Size limit for warehouse usage
            system_privileges: Optional list of system privileges
        """
        self.create_or_alter_role(
            name=name,
            description=description,
            database_access=database_access,
            warehouses=warehouses,
            warehouse_size=warehouse_size,
            system_privileges=system_privileges
        )
