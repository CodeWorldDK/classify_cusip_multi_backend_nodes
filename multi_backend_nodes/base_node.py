
from abc import ABC, abstractmethod
from pydantic import BaseModel, validator, root_validator
from typing import Dict, Any

class ExecutionContext(BaseModel):
    environment: str
    run_id: str

class BaseNode(BaseModel, ABC):
    name: str
    input_datasets: Dict[str, str]
    output_dataset: str
    params: Dict[str, Any]

    @validator("name")
    def name_must_not_be_empty(cls, v):
        if not v.strip():
            raise ValueError("Node name cannot be empty")
        return v

    @validator("input_datasets")
    def must_have_inputs(cls, v):
        if not v:
            raise ValueError("At least one input dataset is required")
        return v

    @root_validator
    def check_output_not_in_inputs(cls, values):
        input_ds = values.get("input_datasets", [])
        output_ds = values.get("output_dataset")
        if output_ds in input_ds:
            raise ValueError("Output dataset cannot be one of the inputs")
        return values
    
    @property
    @abstractmethod
    def node_type(self) -> str:
        pass

    @abstractmethod
    def execute(self, context: ExecutionContext):
        pass
