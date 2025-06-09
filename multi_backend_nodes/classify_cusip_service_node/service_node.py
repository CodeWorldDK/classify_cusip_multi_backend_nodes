
import requests
from base_node import BaseNode, ExecutionContext

class ClassifyCusipServiceNode(BaseNode):
    @property
    def node_type(self):
        return "service"

    def execute(self, context: ExecutionContext):
        print(f"Calling external service for {self.name}")
        url = self.params.get("service_url")
        payload = {
            "inputs": self.input_datasets,
            "output": self.output_dataset,
            "params": self.params
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
