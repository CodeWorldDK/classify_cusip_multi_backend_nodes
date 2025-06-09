import sys
import os
sys.path.append(os.path.dirname(__file__))

from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union
from pydantic import BaseModel

# --- base/context.py ---
class ExecutionContext(BaseModel):
    run_id: str
    environment: str  # "prod", "test"
    params: Dict[str, Union[str, int, float]]


# --- base/node.py ---
class BaseNode(BaseModel, ABC):
    name: str
    input_datasets: List[str]
    output_dataset: str
    context: ExecutionContext

    @property
    @abstractmethod
    def node_type(self) -> str:
        pass

    @abstractmethod
    def execute(self, data_inputs: Dict[str, any]) -> any:
        pass

# --- base/visualize_dag.py ---
from graphviz import Digraph
from base.node import BaseNode

def visualize_dag(nodes: list[BaseNode], output_file="dag_graph"):
    dot = Digraph(comment="Node DAG")
    for node in nodes:
        dot.node(node.name, f"{node.name}\n[{node.node_type}]")
    
    for node in nodes:
        for input_ds in node.input_datasets:
            # Find which node outputs this dataset
            producer = next((n for n in nodes if n.output_dataset == input_ds), None)
            if producer:
                dot.edge(producer.name, node.name)
    
    dot.render(filename=output_file, format="png", cleanup=True)
    print(f"DAG visual saved to {output_file}.png")


# --- nodes/polars_node.py ---
from base.node import BaseNode
import polars as pl

class PolarsNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "polars"

    def execute(self, data_inputs: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        df1 = data_inputs[self.input_datasets[0]]
        df2 = data_inputs[self.input_datasets[1]]
        df_joined = df1.join(df2, on="cusip")
        df_result = df_joined.with_columns([
            pl.when(pl.col("category") == "Equity")
              .then("Equity-Class")
              .otherwise("Other")
              .alias("classification")
        ])
        return df_result


# --- nodes/service_node.py ---
from base.node import BaseNode

class ServiceNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "service"

    def execute(self, data_inputs: Dict[str, any]) -> any:
        print(f"Calling REST API with data from {self.input_datasets[0]}")
        return {"status": "processed"}


# --- nodes/snowpark_node.py ---
from base.node import BaseNode
from snowflake.snowpark import Session, DataFrame as SnowDF

class SnowparkNode(BaseNode):
    session: Session  # Injected at runtime

    @property
    def node_type(self) -> str:
        return "snowpark"

    def execute(self, data_inputs: Dict[str, SnowDF]) -> SnowDF:
        df1 = data_inputs[self.input_datasets[0]]
        df2 = data_inputs[self.input_datasets[1]]
        joined = df1.join(df2, df1["cusip"] == df2["cusip"])
        result = joined.select("cusip", "product", "classification")
        return result


# --- nodes/java_node.py ---
from base.node import BaseNode

class JavaNode(BaseNode):
    @property
    def node_type(self) -> str:
        return "java"

    def execute(self, data_inputs: Dict[str, str]) -> str:
        print(f"Calling Java JAR with input dataset: {self.input_datasets[0]}")
        return "/path/to/output/result.parquet"


# --- base/catalog_service.py ---
def get_input_data(node: BaseNode) -> Dict[str, any]:
    print(f"Fetching input data for node: {node.name}")
    return {ds: f"data_for_{ds}" for ds in node.input_datasets}

def persist_output_data(node: BaseNode, result: any):
    print(f"Persisting output for node {node.name} to {node.output_dataset}")


# --- base/executor.py ---
from base.catalog_service import get_input_data, persist_output_data
from base.node import BaseNode

def execute_node(node: BaseNode):
    print(f"Executing node: {node.name} ({node.node_type})")
    data_inputs = get_input_data(node)
    result = node.execute(data_inputs)
    persist_output_data(node, result)


# --- DAG Orchestration ---
from nodes.polars_node import PolarsNode
from nodes.service_node import ServiceNode
from nodes.snowpark_node import SnowparkNode
from nodes.java_node import JavaNode
from base.context import ExecutionContext
from base.executor import execute_node
from base.dag_visualizer import visualize_dag

def run_dag():
    context = ExecutionContext(run_id="run001", environment="prod", params={})

    node1 = PolarsNode(
        name="classify_cusip_polars",
        input_datasets=["cusip_table", "product_table"],
        output_dataset="classified_cusip",
        context=context
    )

    node2 = ServiceNode(
        name="call_cusip_service",
        input_datasets=["classified_cusip"],
        output_dataset="service_result",
        context=context
    )

    node3 = SnowparkNode(
        name="snowpark_processing",
        input_datasets=["service_result", "external_table"],
        output_dataset="snow_result",
        context=context,
        session=None
    )

    node4 = JavaNode(
        name="final_java_node",
        input_datasets=["snow_result"],
        output_dataset="final_output",
        context=context
    )

    dag_nodes = [node1, node2, node3, node4]

    visualize_dag(dag_nodes)

    for node in dag_nodes:
        execute_node(node)


if __name__ == "__main__":
    run_dag()
