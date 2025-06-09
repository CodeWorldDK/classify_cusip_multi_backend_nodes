from nodes.polars_node import PolarsNode
from nodes.snowpark_node import SnowparkNode
from nodes.service_node import ServiceNode
from nodes.java_node import JavaNode

def execute_node(node: BaseNode):
    print(f"Executing node: {node.name} ({node.node_type})")

    if isinstance(node, PolarsNode):
        data_inputs = get_input_data(node)
        result = node.execute(data_inputs)
        persist_output_data(node, result)

    elif isinstance(node, SnowparkNode):
        if node.session is None:
            raise ValueError("Snowpark session must be initialized for SnowparkNode.")
        data_inputs = get_input_data(node)
        result = node.execute(data_inputs)
        persist_output_data(node, result)

    elif isinstance(node, ServiceNode):
        data_inputs = get_input_data(node)
        result = node.execute(data_inputs)
        persist_output_data(node, result)

    elif isinstance(node, JavaNode):
        data_inputs = get_input_data(node)
        result = node.execute(data_inputs)
        persist_output_data(node, result)

    else:
        raise TypeError(f"Unsupported node type: {type(node)}")
