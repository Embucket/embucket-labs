from typing import Optional, Tuple, List
from snowflake.snowpark.functions import (
    udf,
    col,
    min,
    max,
    count,
    array_agg,
    row_number,
    when,
)
from snowflake.snowpark.types import FloatType, ArrayType
from snowflake.snowpark import Window, DataFrame
import numpy as np


def create_calculate_mode_histogram(session):
    @udf(
        name="calculate_mode_histogram",
        replace=True,
        session=session,
        packages=("numpy",),
        return_type=FloatType(),
        input_types=[ArrayType(FloatType())],
    )
    def calculate_mode_histogram(values: List[float]) -> Optional[float]:
        """Calculate mode using histogram method."""
        if not values:
            return None
        hist, bin_edges = np.histogram(values, bins="auto")
        mode_bin_index = np.argmax(hist)
        return float((bin_edges[mode_bin_index] + bin_edges[mode_bin_index + 1]) / 2)

    return calculate_mode_histogram


def prepare_dataframe(df: DataFrame, occurrence_limit) -> DataFrame:
    """Filter and prepare dataframe for analysis."""
    partition_columns = ["node_id", "materialization", "was_full_refresh"]
    window = Window.partitionBy([col(c) for c in partition_columns]).orderBy(
        col("run_started_at").desc()
    )

    return (
        df.filter(col("node_status") == "success")
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") <= occurrence_limit)
    )


def calculate_node_summary(df: DataFrame, mode_histogram_udf) -> DataFrame:
    """Calculate runtime statistics for each node group."""
    group_columns = ["NODE_ID", "MATERIALIZATION", "WAS_FULL_REFRESH"]

    return df.group_by(group_columns).agg(
        count("*").alias("node_occurrence"),
        min("TOTAL_NODE_RUNTIME").alias("min_runtime"),
        max("TOTAL_NODE_RUNTIME").alias("max_runtime"),
        when(count("*") == 1, min("TOTAL_NODE_RUNTIME"))
        .otherwise(mode_histogram_udf(array_agg("TOTAL_NODE_RUNTIME")))
        .alias("mode_runtime"),
    )


def model(dbt, session):
    """Main model function for dbt execution."""
    # Set up schema
    session.sql(f'USE SCHEMA "{dbt.this.database}".{dbt.this.schema}').collect()

    occurrence_limit = dbt.config.get("occurrence_limit")

    # Create the UDF with the session
    mode_histogram_udf = create_calculate_mode_histogram(session)

    # Process data
    df = dbt.ref("dbt_node_executions")
    processed_df = prepare_dataframe(df, occurrence_limit)
    result_data = calculate_node_summary(processed_df, mode_histogram_udf)

    return result_data
