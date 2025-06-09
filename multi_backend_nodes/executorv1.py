from nodes.base_node import BaseNode, NodeType
from catalog.catalog_service import get_dataset_metadata
from persistor.persistor_service import persist_result

class NodeExecutor:
    def __init__(self, catalog_service, persistor_service):
        self.catalog_service = catalog_service
        self.persistor_service = persistor_service

    def execute(self, node: BaseNode, context: dict):
        print(f"Executing node: {node.name} of type {node.node_type}")

        # Step 1: Get metadata
        input_metadata, output_metadata = self.catalog_service.get_io_metadata(node, context)

        # Step 2: Dispatch logic based on node type
        if node.node_type == NodeType.PYTHON_POLARS:
            df_inputs = self.load_with_polars(input_metadata)
            result = node.run(df_inputs, context)

        elif node.node_type == NodeType.PYTHON_SNOWPARK:
            snowpark_session = context.get("snowpark_session")
            result = node.run(snowpark_session, input_metadata, context)

        elif node.node_type == NodeType.JAVA:
            result = self.invoke_java_process(node, context)

        elif node.node_type == NodeType.SERVICE:
            result = self.call_service_endpoint(node, context)

        else:
            raise ValueError(f"Unknown node type: {node.node_type}")

        # Step 3: Persist result if required
        if node.should_persist:
            self.persistor_service.persist(output_metadata, result, context)

        return result

    def load_with_polars(self, metadata):
        # Example: Load datasets as Polars DataFrames
        import polars as pl
        return {name: pl.read_database(meta["table"]) for name, meta in metadata.items()}

    def invoke_java_process(self, node, context):
        # Could use subprocess or JVM bridge
        pass

    def call_service_endpoint(self, node, context):
        # Hit a REST API using node.params
        pass
