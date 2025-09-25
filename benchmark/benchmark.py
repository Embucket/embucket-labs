import glob
import os
import logging
from typing import Dict, List, Tuple, Any, Optional

from calculate_average import calculate_benchmark_averages
from utils import create_snowflake_connection
from utils import create_embucket_connection
from tpch import parametrize_tpch_queries
from docker_manager import create_docker_manager

from dotenv import load_dotenv
from enum import Enum
import csv
import argparse

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SystemType(Enum):
    EMBUCKET = "embucket"
    SNOWFLAKE = "snowflake"


def get_results_path(platform: str, benchmark_type: str, scale_factor: str,
                     warehouse_or_instance: str, run_number: Optional[int] = None) -> str:
    """Generate path for storing benchmark results."""
    if platform.lower() == "snowflake":
        base_path = f"result/snowflake_{benchmark_type}_results/{scale_factor}/{warehouse_or_instance}"
    elif platform.lower() == "embucket":
        base_path = f"result/embucket_{benchmark_type}_results/{scale_factor}/{warehouse_or_instance}"
    else:
        raise ValueError(f"Unsupported platform: {platform}")

    if run_number is not None:
        return f"{base_path}/{platform.lower()}_results_run_{run_number}.csv"
    return base_path


def save_results_to_csv(results, filename="query_results.csv", platform=None):
    """
    Save benchmark results to CSV file with standardized headers.

    Args:
        results: The query results to save
        filename: Path to save the CSV file
        platform: The platform type ("snowflake" or "embucket")
    """
    headers = ["Query", "Query ID", "Total (ms)", "Rows"]

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        if platform == "embucket":
            # Embucket results format
            query_results, total_time = results
            for row in query_results:
                writer.writerow([row[0], row[1], row[2], row[3]])
            writer.writerow(["TOTAL", "", total_time, ""])
        elif platform == "snowflake":
            # Snowflake results format with simplified query
            total_time = 0
            for row in results:
                query_number = row[0]
                query_id = row[1]
                total_ms = row[2]
                rows = row[3]
                writer.writerow([query_number, query_id, total_ms, rows])
                total_time += total_ms
            writer.writerow(["TOTAL", "", total_time, ""])
        else:
            # Fallback detection for backward compatibility
            if isinstance(results, tuple):
                query_results, total_time = results
                for row in query_results:
                    writer.writerow([row[0], row[1], row[2], row[3]])
                writer.writerow(["TOTAL", "", total_time, ""])
            else:
                total_time = 0
                for row in results:
                    writer.writerow([row[0], row[1], row[2], row[3]])
                    total_time += row[2]
                writer.writerow(["TOTAL", "", total_time, ""])


def run_on_sf(cursor, warehouse, tpch_queries):
    """Run benchmark queries on Snowflake and measure performance."""
    executed_query_ids = []
    query_id_to_number = {}
    results = []

    # Execute queries
    for query_number, query in tpch_queries:
        try:
            logger.info(f"Executing query {query_number}...")

            # Suspend warehouse before each query to ensure clean state
            if warehouse:
                try:
                    cursor.execute(f"ALTER WAREHOUSE {warehouse} SUSPEND;")
                    cursor.execute("SELECT SYSTEM$WAIT(2);")
                    cursor.execute(f"ALTER WAREHOUSE {warehouse} RESUME;")
                except Exception as e:
                    print(f"Warning: Could not suspend/resume warehouse for query {query_number}: {e}")

            cursor.execute(query)
            _ = cursor.fetchall()

            cursor.execute("SELECT LAST_QUERY_ID()")
            query_id = cursor.fetchone()[0]
            if query_id:
                executed_query_ids.append(query_id)
                query_id_to_number[query_id] = query_number
        except Exception as e:
            logger.error(f"Error executing query {query_number}: {e}")

    # Collect performance metrics
    if executed_query_ids:
        query_ids_str = "', '".join(executed_query_ids)
        cursor.execute(f"""
            SELECT
                QUERY_ID,
                TOTAL_ELAPSED_TIME,
                ROWS_PRODUCED
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 1000))
            WHERE QUERY_ID IN ('{query_ids_str}')
            ORDER BY START_TIME
            """)

        query_history = cursor.fetchall()

        for record in query_history:
            query_id = record[0]
            total_time = record[1]
            rows = record[2]
            query_number = query_id_to_number.get(query_id)

            if query_number:
                results.append([
                    query_number,
                    query_id,
                    total_time,
                    rows
                ])

    return results


def run_on_emb(cursor, tpch_queries):
    """Run TPCH queries on Embucket with container restart before each query."""
    docker_manager = create_docker_manager()
    executed_query_ids = []
    query_id_to_number = {}

    for query_number, query in tpch_queries:
        try:
            print(f"Executing query {query_number}...")

            # Restart Embucket container before each query
            print(f"Restarting Embucket container before query {query_number}...")

            if not docker_manager.restart_embucket_container():
                print(f"Failed to restart Embucket container for query {query_number}")
                continue

            print(f"Container restart completed")

            # Create fresh connection after restart
            embucket_connection = create_embucket_connection()
            fresh_cursor = embucket_connection.cursor()

            # Execute the query
            fresh_cursor.execute(query)
            _ = fresh_cursor.fetchall()  # Fetch results but don't store them

            # Close fresh connection after each query
            fresh_cursor.close()
            embucket_connection.close()

        except Exception as e:
            print(f"Error executing query {query_number}: {e}")

            # Try to close connection if it exists
            try:
                if 'fresh_cursor' in locals():
                    fresh_cursor.close()
                if 'embucket_connection' in locals():
                    embucket_connection.close()
            except:
                pass

    # Retrieve query history data from Embucket
    query_results = []
    total_time = 0

    # Get the latest N rows where N is number of queries in the benchmark
    # Filter by successful status and order by start_time
    num_queries = len(tpch_queries)
    history_query = f"""
        SELECT id, duration_ms, result_count, query
        FROM slatedb.history.queries
        WHERE status = 'Successful'
        ORDER BY start_time DESC
        LIMIT {num_queries}
    """

    # Always create fresh connection for history retrieval
    history_connection = create_embucket_connection()
    history_cursor = history_connection.cursor()

    history_cursor.execute(history_query)
    history_results = history_cursor.fetchall()

    # Format the results and calculate total time
    # Results are ordered by start_time DESC, so we reverse to get chronological order
    reversed_results = list(reversed(history_results))

    # Create a list of expected query texts for validation
    expected_queries = [query_text for _, query_text in tpch_queries]

    # Validate we got exactly the expected number of results
    if len(reversed_results) != len(expected_queries):
        raise Exception(f"Expected {len(expected_queries)} query results, but got {len(reversed_results)}")

    for i, record in enumerate(reversed_results):
        query_id = record[0]
        duration_ms = record[1]
        result_count = record[2]
        actual_query = record[3]

        query_number = i + 1

        # Validate that the query text matches what we executed
        expected_query = expected_queries[i]
        if actual_query.strip() != expected_query.strip():
            raise Exception(f"Query text mismatch for query {query_number}. "
                          f"Expected: {expected_query[:100]}... "
                          f"Actual: {actual_query[:100]}...")

        # Add to total time
        total_time += duration_ms

        query_results.append([
            query_number,
            query_id,
            duration_ms,
            result_count
        ])

    history_cursor.close()
    history_connection.close()

    return query_results, total_time


def get_queries_for_benchmark(benchmark_type: str, for_embucket: bool) -> List[Tuple[int, str]]:
    """Get appropriate queries based on the benchmark type."""
    if benchmark_type == "tpch":
        return parametrize_tpch_queries(fully_qualified_names_for_embucket=for_embucket)
    elif benchmark_type == "tpcds":
        raise NotImplementedError("TPC-DS benchmarks not yet implemented")
    else:
        raise ValueError(f"Unsupported benchmark type: {benchmark_type}")


def run_snowflake_benchmark(run_number: int):
    """Run benchmark on Snowflake."""
    # Get benchmark configuration from environment variables
    benchmark_type = os.environ.get("BENCHMARK_TYPE", "tpch")
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
    dataset = os.environ["DATASET_NAME"]
    scale_factor = os.environ["DATASET_SCALE_FACTOR"]

    logger.info(f"Starting Snowflake {benchmark_type} benchmark run {run_number}")
    logger.info(f"Dataset: {dataset}, Schema: {scale_factor}, Warehouse: {warehouse}")

    # Get queries and run benchmark
    queries = get_queries_for_benchmark(benchmark_type, for_embucket=False)

    sf_connection = create_snowflake_connection()
    sf_cursor = sf_connection.cursor()

    # Disable query result caching for benchmark
    sf_cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")

    sf_results = run_on_sf(sf_cursor,warehouse, queries)

    results_path = get_results_path("snowflake", benchmark_type, scale_factor, warehouse, run_number)
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    save_results_to_csv(sf_results, filename=results_path, platform="snowflake")

    logger.info(f"Snowflake benchmark results saved to: {results_path}")

    sf_cursor.close()
    sf_connection.close()

    # Check if we have 3 CSV files ready and calculate averages if so
    results_dir = get_results_path("snowflake", benchmark_type, scale_factor, warehouse)
    csv_files = glob.glob(os.path.join(results_dir, "snowflake_results_run_*.csv"))
    if len(csv_files) == 3:
        logger.info("Found 3 CSV files. Calculating averages...")
        calculate_benchmark_averages(
            scale_factor,
            warehouse,
            SystemType.SNOWFLAKE,
            benchmark_type
        )

    return sf_results


def run_embucket_benchmark(run_number: int):
    """Run benchmark on Embucket with container restarts."""
    # Get benchmark configuration from environment variables
    benchmark_type = os.environ.get("BENCHMARK_TYPE", "tpch")
    instance = os.environ["EMBUCKET_INSTANCE"]
    dataset = os.environ.get("EMBUCKET_DATASET", os.environ["DATASET_NAME"])
    scale_factor = os.environ["DATASET_SCALE_FACTOR"]

    logger.info(f"Starting Embucket {benchmark_type} benchmark run {run_number}")
    logger.info(f"Instance: {instance}, Dataset: {dataset}, Scale Factor: {scale_factor}")

    # Get queries and docker manager
    queries = get_queries_for_benchmark(benchmark_type, for_embucket=True)
    docker_manager = create_docker_manager()

    # Run benchmark
    emb_results = run_on_emb(docker_manager, queries)

    results_path = get_results_path("embucket", benchmark_type, scale_factor, instance, run_number)
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    save_results_to_csv(emb_results, filename=results_path, platform="embucket")

    logger.info(f"Embucket benchmark results saved to: {results_path}")

    # Check if we have 3 CSV files ready and calculate averages
    results_dir = get_results_path("embucket", benchmark_type, scale_factor, instance)
    csv_files = glob.glob(os.path.join(results_dir, "embucket_results_run_*.csv"))
    if len(csv_files) == 3:
        logger.info("Found 3 CSV files. Calculating averages...")
        calculate_benchmark_averages(
            scale_factor,
            instance,
            SystemType.EMBUCKET,
            benchmark_type
        )

    return emb_results


def display_comparison(sf_results, emb_results):
    """Display comparison of query times between platforms."""
    # Process Snowflake results
    sf_query_times = {}
    for row in sf_results:
        query_number = row[0]
        total_time = row[4]  # Total time column
        sf_query_times[query_number] = total_time

    # Process Embucket results
    emb_query_times = {}
    query_results, _ = emb_results
    for row in query_results:
        query_number = row[0]
        query_time = row[2]  # Query time column
        emb_query_times[query_number] = query_time

    # Check for common queries
    common_queries = set(sf_query_times.keys()).intersection(set(emb_query_times.keys()))
    if not common_queries:
        logger.warning("No common queries to compare between platforms")
        return

    # Log comparison
    logger.info("Performance comparison (Snowflake vs Embucket):")
    for query in sorted(common_queries):
        sf_time = sf_query_times[query]
        emb_time = emb_query_times[query]
        ratio = sf_time / emb_time if emb_time > 0 else float('inf')
        logger.info(f"Query {query}: Snowflake {sf_time:.2f}ms, Embucket {emb_time:.2f}ms, Ratio: {ratio:.2f}x")


def run_benchmark(run_number: int, platform_enum: Optional[SystemType]):
    """Run benchmarks on the specified platform."""
    if platform_enum == SystemType.EMBUCKET:
        run_embucket_benchmark(run_number)
    elif platform_enum == SystemType.SNOWFLAKE:
        run_snowflake_benchmark(run_number)
    else:
        raise ValueError("Unsupported or missing platform_enum")


def parse_args():
    """Parse command line arguments for benchmark configuration."""
    parser = argparse.ArgumentParser(description="Run benchmarks on Snowflake and/or Embucket")
    parser.add_argument("--platform", choices=["snowflake", "embucket", "both"], default="both")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--benchmark-type", choices=["tpch", "tpcds"], default=os.environ.get("BENCHMARK_TYPE", "tpch"))
    parser.add_argument("--dataset-name", help="Override the DATASET_NAME environment variable")
    parser.add_argument("--scale-factor", help="Override the DATASET_SCALE_FACTOR environment variable")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Override environment variables if specified in args
    if args.benchmark_type != os.environ.get("BENCHMARK_TYPE", "tpch"):
        os.environ["BENCHMARK_TYPE"] = args.benchmark_type

    if args.dataset_name:
        os.environ["DATASET_NAME"] = args.dataset_name

    if args.scale_factor:
        os.environ["DATASET_SCALE_FACTOR"] = args.scale_factor
