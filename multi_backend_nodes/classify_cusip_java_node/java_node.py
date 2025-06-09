
from base_node import BaseNode, ExecutionContext

class ClassifyCusipJavaNode(BaseNode):
    @property
    def node_type(self):
        return "java"

    def execute(self, context: ExecutionContext):
        print(f"Calling Java JAR for {self.name} with params: {self.params}")
        