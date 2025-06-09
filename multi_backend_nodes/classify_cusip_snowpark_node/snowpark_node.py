
from base_node import BaseNode, ExecutionContext

class ClassifyCusipSnowparkNode(BaseNode):
    @property
    def node_type(self):
        return "snowpark"

    def execute(self, context: ExecutionContext):
        print(f"Executing {self.name} using Snowpark session for env: {context.environment}")
        # Executor would inject Snowpark DataFrame references
