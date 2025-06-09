
from abc import ABC, abstractmethod
from pydantic import BaseModel
from typing import Dict, Any

class ExecutionContext(BaseModel):
    environment: str
    run_id: str

class BaseNode(BaseModel, ABC):
    name: str
    input_datasets: Dict[str, str]
    output_dataset: str
    params: Dict[str, Any]

    @property
    @abstractmethod
    def node_type(self) -> str:
        pass

    @abstractmethod
    def execute(self, context: ExecutionContext):
        pass
