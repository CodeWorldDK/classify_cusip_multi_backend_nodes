
import polars as pl
from typing import Dict
from base_node import BaseNode, ExecutionContext

class ClassifyCusipPolarsNode(BaseNode):
    @property
    def node_type(self):
        return "pandas"

    def transform(self, input_dfs: Dict[str, pl.DataFrame]) -> pl.DataFrame:
        cusips_df = input_dfs["cusip_list"]
        categories_df = input_dfs["product_category"]
        result = cusips_df.join(categories_df, on="cusip", how="left")
        result = result.with_columns([
            pl.when(pl.col("product_category") == "Equity")
              .then("High Risk")
              .otherwise("Low Risk")
              .alias("classification")
        ])
        return result

    def execute(self, context: ExecutionContext):
        print(f"Executing {self.name} in env: {context.environment}")
        # Dummy input data loading
        input_dfs = {}  # would be filled by executor
        result_df = self.transform(input_dfs)
        print("Transformed data:", result_df)
