from aws_cdk import Stack
from constructs import Construct
from typing import Any, Dict


class RoleStack(Stack):
    def __init__(self, scope: Construct, id: str, snowflake_secret: Dict[str, Any], **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # TODO: Implement role creation
        pass
