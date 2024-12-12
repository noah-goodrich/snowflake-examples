from aws_cdk import Stack
from constructs import Construct

from stacks.admin import Admin


class FoundationStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Create admin stack
        admin = Admin(self, "Admin")
        admin.deploy()
