from aws_cdk import Stack
from constructs import Construct
from typing import Any, Dict

from .storage import StorageIntegrationStack


class CrossPlatformStack(Stack):
    def __init__(self, scope: Construct, id: str, snowflake_secret: Dict[str, Any], **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create storage integration stack
        storage = StorageIntegrationStack(self, "StorageIntegration",
                                          snowflake_secret=snowflake_secret)
