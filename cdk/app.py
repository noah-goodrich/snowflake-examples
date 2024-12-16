#!/usr/bin/env python3
from aws_cdk import App, Environment
from stacks.foundation import Foundation

app = App()

env = Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region")
)

Foundation(app, "Foundation", env=env)

app.synth()
