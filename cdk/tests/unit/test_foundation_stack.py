import aws_cdk as core
import aws_cdk.assertions as assertions

from foundation.foundation_stack import FoundationStack

# example tests. To run these tests, uncomment this file along with the example
# resource in foundation/foundation_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = FoundationStack(app, "foundation")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
