from aws_cdk import Stack
from constructs import Construct
from .secret_stack import SecretStack
from .snowflake_stack import SnowflakeStack


class FoundationStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create Secret Stack first
        self.secret_stack = SecretStack(self, "SecretStack")

        # Create Snowflake Stack with reference to secrets
        self.snowflake_stack = SnowflakeStack(
            self,
            "SnowflakeStack",
            secrets=self.secret_stack.secrets
        )
