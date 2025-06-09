# executor.py
import importlib
from base_node import ExecutionContext
from typing import Dict


def load_node(node_class_path: str):
    """
    Dynamically loads the node class from its module path.
    """
    module_path, class_name = node_class_path.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def execute_node(node_config: Dict, context_dict: Dict):
    """
    Main executor function.
    Loads and executes the node based on provided configuration.
    """
    NodeClass = load_node(node_config['class_path'])

    # Build execution context
    context = ExecutionContext(**context_dict)

    # Instantiate and run node
    node_instance = NodeClass(
        name=node_config['name'],
        node_type=node_config['type'],
        params=node_config.get('params', {})
    )

    result = node_instance.run(context)
    return result


if __name__ == '__main__':
    # Example usage for local testing
    node_config = {
        'name': 'classify_cusip',
        'type': 'polars',
        'class_path': 'nodes.polars.polars_node.ClassifyCusipPolarsNode',
        'params': {
            'classification_ruleset': 'R1'
        }
    }

    context_dict = {
        'env': 'prod',
        'input_datasets': {
            'cusip_list': 'schema_prod.cusip_list',
            'product_category': 'schema_prod.product_category'
        },
        'output_dataset': 'schema_prod.classified_cusip',
        'execution_date': '2025-06-09'
    }

    result = execute_node(node_config, context_dict)
    print("Execution result:", result)
