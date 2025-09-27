import glob
import os
import logging
from typing import Dict, List, Tuple, Any, Optional

from calculate_average import calculate_benchmark_averages
from utils import create_snowflake_connection
from utils import create_embucket_connection
from tpch import parametrize_tpch_queries
from docker_manager import create_docker_manager
from constants import SystemType

from dotenv import load_dotenv
import csv
import argparse

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_results_path(system: SystemType, benchmark_type: str, dataset_path: str,
                     instance: str, warehouse_size: str = None, run_number: Optional[int] = None,
                     cached: bool = False) -> str:
    """Generate path for storing benchmark results."""
    cache_folder = "cached" if cached else "no_cache"

    if system == SystemType.SNOWFLAKE:
        # Use warehouse size in the path instead of warehouse name
        base_path = f"result/snowflake_{benchmark_type}_results/{dataset_path}/{warehouse_size}/{cache_folder}"
    elif system == SystemType.EMBUCKET:
        base_path = f"result/embucket_{benchmark_type}_results/{dataset_path}/{instance}/{cache_folder}"
    else:
        raise ValueError(f"Unsupported system: {system}")

    if run_number is not None:
        return f"{base_path}/{system.value}_results_run_{run_number}.csv"
    return base_path


def save_results_to_csv(results, filename="query_results.csv", system=None):
    """
    Save benchmark results to CSV file with standardized headers.

    Args:
        results: The query results to save
        filename: Path to save the CSV file
        system: The system type (SystemType.SNOWFLAKE or SystemType.EMBUCKET)
    """
    headers = ["Query", "Query ID", "Total (ms)", "Rows"]

    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        if system == SystemType.EMBUCKET:
            # Embucket results format
            query_results, total_time = results
            for row in query_results:
                writer.writerow([row[0], row[1], row[2], row[3]])
            writer.writerow(["TOTAL", "", total_time, ""])
        elif system == SystemType.SNOWFLAKE:
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


def run_on_sf(cursor, warehouse, tpch_queries, cache=False):
    """Run benchmark queries on Snowflake and measure performance."""
    executed_query_ids = []
    query_id_to_number = {}
    results = []

    # Execute queries
    for query_number, query in tpch_queries:
        try:
            logger.info(f"Executing query {query_number}...")

            # Suspend warehouse before each query to ensure clean state (skip if no_cache is True)
            if not cache:
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


def run_on_emb(tpch_queries, cache=False):
    """Run TPCH queries on Embucket with container restart before each query."""
    docker_manager = create_docker_manager()
    executed_query_ids = []
    query_id_to_number = {}

    if not cache:
        logger.info("Embucket benchmark running with container restarts (no cache)")
        # Connection will be created per query after container restart
        embucket_connection = None
    else:
        logger.info("Embucket benchmark running with caching (no container restarts)")
        # Create a single connection when using cache
        embucket_connection = create_embucket_connection()

    for query_number, query in tpch_queries:
        try:
            print(f"Executing query {query_number}...")

            # Restart Embucket container before each query (skip if cache is True)
            if not cache:
                print(f"Restarting Embucket container before query {query_number}...")

                if not docker_manager.restart_embucket_container():
                    print(f"Failed to restart Embucket container for query {query_number}")
                    continue

                print(f"Container restart completed")

                # Create fresh connection after restart
                embucket_connection = create_embucket_connection()

            # Now embucket_connection should be properly initialized in both cases
            fresh_cursor = embucket_connection.cursor()

            # Execute the query
            fresh_cursor.execute(query)
            _ = fresh_cursor.fetchall()  # Fetch results but don't store them

            # Close fresh connection after each query only if we're restarting
            if not cache:
                fresh_cursor.close()
                embucket_connection.close()
                embucket_connection = None

        except Exception as e:
            print(f"Error executing query {query_number}: {e}")

            # Try to close connection if it exists and we're in no_cache mode
            if not cache and embucket_connection:
                try:
                    if 'fresh_cursor' in locals():
                        fresh_cursor.close()
                    embucket_connection.close()
                    embucket_connection = None
                except:
                    pass

    # Close the connection if we're using cache
    if cache and embucket_connection:
        try:
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


def run_snowflake_benchmark(run_number: int, cache: bool = False):
    """Run benchmark on Snowflake."""
    # Get benchmark configuration from environment variables
    benchmark_type = os.environ.get("BENCHMARK_TYPE", "tpch")
    warehouse = os.environ["SNOWFLAKE_WAREHOUSE"]
    warehouse_size = os.environ["SNOWFLAKE_WAREHOUSE_SIZE"]
    dataset_path = os.environ["DATASET_PATH"]

    logger.info(f"Starting Snowflake {benchmark_type} benchmark run {run_number}")
    logger.info(f"Dataset: {dataset_path}, Warehouse: {warehouse}, Size: {warehouse_size}")

    # Get queries and run benchmark
    queries = get_queries_for_benchmark(benchmark_type, for_embucket=False)

    sf_connection = create_snowflake_connection()
    sf_cursor = sf_connection.cursor()

    # Control query result caching for benchmark
    if cache:
        logger.info("Using cached results for Snowflake queries")
        sf_cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = TRUE;")
    else:
        logger.info("Disabling cached results for Snowflake queries")
        sf_cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")

    sf_results = run_on_sf(sf_cursor, warehouse, queries, cache=cache)

    results_path = get_results_path(SystemType.SNOWFLAKE, benchmark_type, dataset_path,
                                  warehouse, warehouse_size, run_number, cached=cache)
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    save_results_to_csv(sf_results, filename=results_path, system=SystemType.SNOWFLAKE)

    logger.info(f"Snowflake benchmark results saved to: {results_path}")

    sf_cursor.close()
    sf_connection.close()

    # Check if we have 3 CSV files ready and calculate averages if so
    results_dir = get_results_path(SystemType.SNOWFLAKE, benchmark_type, dataset_path,
                                 warehouse, warehouse_size, cached=cache)
    csv_files = glob.glob(os.path.join(results_dir, "snowflake_results_run_*.csv"))
    if len(csv_files) == 3:
        logger.info("Found 3 CSV files. Calculating averages...")
        calculate_benchmark_averages(
            dataset_path,
            warehouse_size,
            SystemType.SNOWFLAKE,
            benchmark_type,
            cached=cache
        )

    return sf_results



def run_embucket_benchmark(run_number: int, cache: bool = True):
    """Run benchmark on Embucket with container restarts."""
    # Get benchmark configuration from environment variables
    benchmark_type = os.environ.get("BENCHMARK_TYPE", "tpch")
    instance = os.environ["EMBUCKET_INSTANCE"]
    dataset_path = os.environ.get("EMBUCKET_DATASET_PATH", os.environ["DATASET_PATH"])

    logger.info(f"Starting Embucket {benchmark_type} benchmark run {run_number}")
    logger.info(f"Instance: {instance}, Dataset: {dataset_path}")

    # Get queries and docker manager
    queries = get_queries_for_benchmark(benchmark_type, for_embucket=True)

    # Run benchmark
    emb_results = run_on_emb(queries, cache=cache)

    results_path = get_results_path(SystemType.EMBUCKET, benchmark_type, dataset_path,
                                  instance, run_number=run_number, cached=cache)
    os.makedirs(os.path.dirname(results_path), exist_ok=True)
    save_results_to_csv(emb_results, filename=results_path, system=SystemType.EMBUCKET)
    logger.info(f"Embucket benchmark results saved to: {results_path}")

    # Check if we have 3 CSV files ready and calculate averages
    results_dir = get_results_path(SystemType.EMBUCKET, benchmark_type, dataset_path,
                                 instance, cached=cache)
    csv_files = glob.glob(os.path.join(results_dir, "embucket_results_run_*.csv"))
    if len(csv_files) == 3:
        logger.info("Found 3 CSV files. Calculating averages...")
        calculate_benchmark_averages(
            dataset_path,
            instance,
            SystemType.EMBUCKET,
            benchmark_type,
            cached=cache
        )

    return emb_results


def display_comparison(sf_results, emb_results):
    """Display comparison of query times between systems."""
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
        logger.warning("No common queries to compare between systems")
        return

    # Log comparison
    logger.info("Performance comparison (Snowflake vs Embucket):")
    for query in sorted(common_queries):
        sf_time = sf_query_times[query]
        emb_time = emb_query_times[query]
        ratio = sf_time / emb_time if emb_time > 0 else float('inf')
        logger.info(f"Query {query}: Snowflake {sf_time:.2f}ms, Embucket {emb_time:.2f}ms, Ratio: {ratio:.2f}x")


def run_benchmark(run_number: int, system_enum: Optional[SystemType], no_cache: bool = True):
    """Run benchmarks on the specified system."""
    if system_enum == SystemType.EMBUCKET:
        run_embucket_benchmark(run_number, cache=not no_cache)
    elif system_enum == SystemType.SNOWFLAKE:
        run_snowflake_benchmark(run_number, cache=not no_cache)
    else:
        raise ValueError("Unsupported or missing system_enum")


def parse_args():
    """Parse command line arguments for benchmark configuration."""
    parser = argparse.ArgumentParser(description="Run benchmarks on Snowflake and/or Embucket")
    parser.add_argument("--system", choices=["snowflake", "embucket", "both"], default="both")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--benchmark-type", choices=["tpch", "tpcds"], default=os.environ.get("BENCHMARK_TYPE", "tpch"))
    parser.add_argument("--dataset-path", help="Override the DATASET_PATH environment variable")
    parser.add_argument("--no-cache", action="store_true", help="Disable caching (force warehouse suspend and USE_CACHED_RESULT=False for Snowflake, force container restart for Embucket)")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Override environment variables if specified in args
    if args.benchmark_type != os.environ.get("BENCHMARK_TYPE", "tpch"):
        os.environ["BENCHMARK_TYPE"] = args.benchmark_type

    if args.dataset_path:
        os.environ["DATASET_PATH"] = args.dataset_path

    # Execute benchmarks based on system selection
    if args.system == "snowflake":
        for run in range(1, args.runs + 1):
            run_benchmark(run, SystemType.SNOWFLAKE, no_cache=args.no_cache)
    elif args.system == "embucket":
        for run in range(1, args.runs + 1):
            run_benchmark(run, SystemType.EMBUCKET, no_cache=args.no_cache)
    elif args.system == "both":
        for run in range(1, args.runs + 1):
            logger.info(f"Starting benchmark run {run} for both systems")
            run_benchmark(run, SystemType.SNOWFLAKE, no_cache=args.no_cache)
            run_benchmark(run, SystemType.EMBUCKET, no_cache=args.no_cache)