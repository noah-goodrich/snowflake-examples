#!/usr/bin/env python3
from aws_cdk import App
from stacks.secret_stack import SecretStack
from stacks.snowflake_stack import SnowflakeStack
from stacks.warehouse_stack import WarehouseStack

app = App()

# Create stacks
secret_stack = SecretStack(app, "SecretStack")
snowflake_stack = SnowflakeStack(
    app, "SnowflakeStack", secrets=secret_stack.secrets)
warehouse_stack = WarehouseStack(app, "WarehouseStack", env="DEV")

app.synth()
