from aws_cdk import Stack
from constructs import Construct
from typing import Any, Dict

from stacks.snowflake.stack import SnowflakeStack
from stacks.cross_platform.stack import CrossPlatformStack
from stacks.secrets.secret import SecretStack


class FoundationStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create secrets stack
        secret_stack = SecretStack(self, "Secrets")

        # # Create Snowflake parent stack
        # snowflake = SnowflakeStack(self, "Snowflake",
        #                            snowflake_secret=secret_stack.secret)

        # # Create Cross-platform parent stack
        # cross_platform = CrossPlatformStack(self, "CrossPlatform",
        #                                     snowflake_secret=secret_stack.secret)

        # # Add dependencies
        # cross_platform.add_dependency(snowflake)
