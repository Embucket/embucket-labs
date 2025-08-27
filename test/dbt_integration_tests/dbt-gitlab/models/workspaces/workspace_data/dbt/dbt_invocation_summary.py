import snowflake.snowpark.functions as F
from snowflake.snowpark.types import (
    StringType,
    TimestampType,
    ArrayType,
    StructType,
    StructField,
)
import networkx as nx
from typing import Dict, List, Any
from functools import reduce

# Type aliases for better readability
NodeId = str
BuildTime = float
InvocationId = str


def create_graph_from_rows(rows: List[Dict]) -> nx.DiGraph:
    """Create a directed graph from rows in a functional way."""

    def add_node(G: nx.DiGraph, row: Dict) -> nx.DiGraph:
        G.add_node(
            row["NODE_ID"],
            invocation=row["COMMAND_INVOCATION_ID"],
            build_time=row["TOTAL_NODE_RUNTIME"],
        )
        return G

    def add_edges(G: nx.DiGraph, row: Dict) -> nx.DiGraph:
        if row["DEPENDS_ON_NODES"]:
            parents = [p.strip() for p in row["DEPENDS_ON_NODES"].split(",")]
            for parent in parents:
                if any(parent == node for node in G.nodes()):
                    G.add_edge(
                        parent,
                        row["NODE_ID"],
                    )
        return G

    # Create graph using function composition
    return reduce(lambda g, row: add_edges(add_node(g, row), row), rows, nx.DiGraph())


def calculate_critical_path(G: nx.DiGraph) -> Dict[str, Any]:
    # Find all simple paths in the graph
    start_nodes = [node for node, in_degree in G.in_degree() if in_degree == 0]
    end_nodes = [node for node, out_degree in G.out_degree() if out_degree == 0]
    if start_nodes == end_nodes:
        all_paths = [start_nodes[i : i + 1] for i in range(0, len(start_nodes))]
    else:
        all_paths = []
        for node in start_nodes:
            paths = nx.all_simple_paths(G, node, end_nodes)
            all_paths.extend(paths)

    path_lengths = []
    for path in all_paths:
        length = sum(G.nodes[path[i]]["build_time"] for i in range(len(path) - 1))
        path_lengths.append(
            {
                "critical_path": path,
                "path_length": length,
            }
        )

    # Sort paths by length in descending order and get the top path
    return sorted(path_lengths, key=lambda x: x.get("path_length"), reverse=True)[0]


def format_mermaid_gantt(path_data: List[tuple]) -> str:
    """Format node execution data into a Mermaid gantt chart string."""
    header = [
        "gantt",
        "    dateFormat HH:mm",
        "    axisFormat %H:%M",
        "    title Critical Path",
    ]

    def format_task(task_data: tuple, is_first: bool) -> str:
        node, duration = task_data
        return (
            f"    {node} : 0, {duration}s" if is_first else f"    {node} : {duration}s"
        )

    tasks = [format_task(task_data, i == 0) for i, task_data in enumerate(path_data)]

    return "\n".join(header + tasks)


def process_invocations(df) -> List[Dict[str, Any]]:
    """Process all invocations in a functional way with minimal collect() calls."""
    # Single collect() call to get all required data
    rows = df.select(
        "node_id",
        "command_invocation_id",
        "total_node_runtime",
        "depends_on_nodes",
        "run_started_at",
    ).collect()

    # Group rows by invocation_id
    invocation_groups = {}
    for row in rows:
        inv_id = row["COMMAND_INVOCATION_ID"]
        if inv_id not in invocation_groups:
            invocation_groups[inv_id] = {
                "rows": [],
                "run_started_at": row["RUN_STARTED_AT"],
            }
        invocation_groups[inv_id]["rows"].append(row)

    def process_group(inv_id: str, group_data: Dict) -> Dict[str, Any]:
        # Create DAG and calculate critical path
        dag = create_graph_from_rows(group_data["rows"])
        critical_path_info = calculate_critical_path(dag)
        path_data = [
            (node, dag.nodes[node]["build_time"])
            for node in critical_path_info["critical_path"]
        ]

        return {
            "invocation_id": inv_id,
            "run_started_at": group_data["run_started_at"],
            "critical_path_node_ids": critical_path_info["critical_path"],
            "critical_path_mermaid": format_mermaid_gantt(path_data),
        }

    return [
        process_group(inv_id, group_data)
        for inv_id, group_data in invocation_groups.items()
    ]


def prepare_dataframe(df, is_incremental: bool, max_from_this: str, session):
    """Prepare and filter the dataframe based on incremental loading conditions."""
    max_date = session.sql(max_from_this).collect()[0][0] if is_incremental else None

    return (
        df.filter(df.run_started_at > max_date)
        if is_incremental
        else df.filter(F.col("run_started_at") >= F.current_date())
    ).withColumn(
        "depends_on_nodes", F.array_to_string(F.col("depends_on_nodes"), F.lit(","))
    )


def model(dbt, session):
    """Main model function for dbt execution."""
    dbt.config(enabled=False)
    dbt.config(packages=["networkx"])
    dbt.config(materialized="incremental")
    dbt.config(incremental_strategy="append")
    dbt.config(on_schema_change="sync_all_columns")
    dbt.config(full_refresh=False)

    # Schema not defined in target, setting it as part of the model
    session.sql(f'USE SCHEMA "{dbt.this.database}".{dbt.this.schema}').collect()

    df = dbt.ref("dbt_node_executions")
    max_from_this = f"SELECT MAX(run_started_at) FROM {dbt.this}"

    processed_df = prepare_dataframe(df, dbt.is_incremental, max_from_this, session)

    # Starting with empty data incase there are no invocations to process
    result_data = []
    result_data.extend(process_invocations(processed_df))

    # Defining the schema for the final result, needed for an empty result
    schema = StructType(
        [
            StructField("invocation_id", StringType()),
            StructField("run_started_at", TimestampType()),
            StructField("critical_path_node_ids", ArrayType(StringType())),
            StructField("critical_path_mermaid", StringType()),
        ]
    )

    return session.create_dataframe(result_data, schema=schema)
