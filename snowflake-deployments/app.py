#!/usr/bin/env python3
from aws_cdk import App, Environment
from stacks.stack import FoundationStack

app = App()

env = Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region")
)

FoundationStack(app, "Foundation", env=env)

app.synth()
