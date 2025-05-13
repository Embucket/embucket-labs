import json
import os
import glob
import time
from collections import defaultdict
from typing import Any, Generator, Optional, Dict
import gc
import csv

import requests
from dotenv import load_dotenv

from prettytable import PrettyTable

from snowflake.connector import (SnowflakeConnection)
from snowflake.connector import connect

from slt_runner.logger import logger

from slt_runner import (
    SQLParserException,
    SQLLogicParser,
    SQLLogicTest, SkipIf,
)

from slt_runner.result import (
    TestException,
    SQLLogicRunner,
    SQLLogicDatabase,
    SQLLogicContext,
    ExecuteResult,
)

from slt_runner.display_test_statistics import display_page_results, display_category_results


# ANSI color codes
GREEN = "\033[92m"
RED = "\033[91m"
GREY = '\033[90m'
RESET = "\033[0m"

def extract_file_name(file_path: str) -> str:
    return os.path.splitext(os.path.basename(file_path))[0]


class EmbucketHelper:
    def __init__(self, config):
        self.config = config

    def setup_database_environment(self,
                                   embucket_url,
                                   database,
                                   schema
                                   ):
        headers = {'Content-Type': 'application/json'}

        query = f"DROP SCHEMA IF EXISTS {schema} CASCADE"
        requests.request(
            "POST", f"{embucket_url}/ui/queries",
            headers=headers,
            data=json.dumps({"query": query})
        ).json()

        payload = json.dumps({
                "type": "memory",
                "ident": "local",
            })
        requests.request("POST", f'{embucket_url}/v1/metastore/volumes', headers=headers, data=payload).json()

        payload = json.dumps({
            "ident": database,
            "volume": "local"
        })
        requests.request("POST", f'{embucket_url}/v1/metastore/databases', headers=headers, data=payload).json()

        query = f"CREATE SCHEMA IF NOT EXISTS {database}.{schema}"
        requests.request(
            "POST", f"{embucket_url}/ui/queries",
            headers=headers,
            data=json.dumps({"query": query})
        ).json()

def reset_database(config, con: SnowflakeConnection):
    if 'embucket' not in config:
        db_name = config['database']
        con.execute(f"DROP DATABASE IF EXISTS {db_name};")
        con.execute(f"CREATE DATABASE {db_name};")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {config["schema"]};")

# This is pretty much just a VM
class SQLLogicTestExecutor(SQLLogicRunner):
    def __init__(self,
                 config: Dict[str, str],
                 default_test_dir: Optional[str] = None,
                 benchmark_mode: bool = False,
                 run_mode: str = None  # Add run_mode parameter
                 ):
        super().__init__(config, default_test_dir)
        self.SKIPPED_TESTS = set([])
        self.skip_log = []
        self.embucket = None
        self.benchmark_mode = benchmark_mode
        self.run_mode = run_mode  # Store the run mode
        self.connection_pool = None  # Store connection pool for hot runs
        self.executed_query_ids = []

    # capturing query ID after execution
    def capture_query_id(self, connection):
        """Capture the ID of the most recently executed query"""
        try:
            # Connection parameter is already a cursor, use it directly
            connection.execute("SELECT LAST_QUERY_ID()")
            query_id = connection.fetchone()[0]
            if query_id:
                self.executed_query_ids.append(query_id)
                return query_id
        except Exception as e:
            logger.error(f"Error capturing query ID: {e}")
        return None


    def analyze_query_performance(self, run_mode):
        """Analyze performance of specific queries executed during the test run"""
        if not self.executed_query_ids:
            logger.warning("No query IDs collected for analysis")
            return None

        try:
            # Create connection to query metadata
            con = connect(**self.config)

            # Build query to fetch statistics for specific query IDs
            query_ids_str = "', '".join(self.executed_query_ids)
            fetch_statistics_query = f"""
            SELECT
                QUERY_ID,
                QUERY_TYPE,
                COMPILATION_TIME,
                EXECUTION_TIME,
                TOTAL_ELAPSED_TIME,
                ROWS_PRODUCED,
                QUERY_TEXT
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_ID IN ('{query_ids_str}')
            ORDER BY START_TIME
            """

            results = []
            with con.cursor() as cursor:
                cursor.execute(fetch_statistics_query)
                results = cursor.fetchall()

            logger.info(f"Analyzed {len(results)} query executions from run mode: {run_mode}")

            # Display and compute analytics
            if results:
                # Create pretty table for display
                query_table = PrettyTable()
                query_table.field_names = ["Query ID", "Type", "Compilation (ms)", "Execution (ms)",
                                           "Total (ms)", "Rows", "Query Text"]

                # Add rows to the table
                for row in results:
                    query_id = row[0]
                    query_type = row[1]
                    compilation_ms = float(row[2]) if row[2] is not None else 0.0
                    execution_ms = float(row[3]) if row[3] is not None else 0.0
                    total_ms = float(row[4]) if row[4] is not None else 0.0
                    rows_produced = row[5] if row[5] is not None else 0
                    query_text = row[6][:30].replace("\n", " ").replace("\t", " ")

                    query_table.add_row([
                        query_id,
                        query_type,
                        f"{compilation_ms:.2f}",
                        f"{execution_ms:.2f}",
                        f"{total_ms:.2f}",
                        rows_produced,
                        query_text
                    ])

                # Print the table
                print(f"\nQuery Performance Statistics ({run_mode.upper()} run):")
                print(query_table)

                # Calculate average metrics
                avg_compilation = sum(float(row[2]) if row[2] is not None else 0.0 for row in results) / len(results)
                avg_execution = sum(float(row[3]) if row[3] is not None else 0.0 for row in results) / len(results)
                avg_total = sum(float(row[4]) if row[4] is not None else 0.0 for row in results) / len(results)
                avg_rows = sum(row[5] if row[5] is not None else 0 for row in results) / len(results)

                # Prepare statistics to return
                stats = {
                    'count': len(results),
                    'avg_compilation_ms': avg_compilation,
                    'avg_execution_ms': avg_execution,
                    'avg_total_ms': avg_total,
                    'avg_rows': avg_rows
                }

                # Save results to CSV
                with open(f"{run_mode}_query_performance.csv", "w", newline="") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(query_table.field_names)
                    for row in results:
                        writer.writerow([
                            row[0], row[1],
                            f"{float(row[2]) if row[2] is not None else 0.0:.2f}",
                            f"{float(row[3]) if row[3] is not None else 0.0:.2f}",
                            f"{float(row[4]) if row[4] is not None else 0.0:.2f}",
                            row[5] if row[5] is not None else 0,
                            row[6]
                        ])

                logger.info(f"Saved {len(results)} query performance records to {run_mode}_query_performance.csv")
                return stats

            return None

        except Exception as e:
            logger.error(f"Error analyzing query performance: {e}")
            return None

        finally:
            if 'con' in locals() and con:
                con.close()
            # Clear the query IDs after analysis
            self.executed_query_ids = []

    def suspend_warehouse(self, delay_seconds: int = 10):
        """Suspend the warehouse and introduce a delay without affecting performance results.
        Suspending the warehouse clears the warehouse cache. However, immediately resuming
        the warehouse might allocate the same compute resources, potentially retaining some cache"""
        try:
            con = connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=self.config['warehouse']
            )
            with con.cursor() as cursor:
                # Check warehouse state
                cursor.execute(f"SHOW WAREHOUSES LIKE '{self.config['warehouse']}'")
                warehouse_info = cursor.fetchone()
                if warehouse_info and warehouse_info[1] != 'SUSPENDED':
                    start_time = time.time()
                    cursor.execute(f"ALTER WAREHOUSE {self.config['warehouse']} SUSPEND")
                    elapsed_time = time.time() - start_time
                    logger.info(f"Warehouse '{self.config['warehouse']}' suspended in {elapsed_time:.2f} seconds.")
                else:
                    logger.info(f"Warehouse '{self.config['warehouse']}' is already suspended.")
            con.close()
            time.sleep(delay_seconds)  # Apply delay after performance measurement
        except Exception as e:
            logger.error(f"Error suspending warehouse '{self.config['warehouse']}': {e}")

    def get_test_directory(self):
        """Return the test directory path."""
        return self.default_test_dir

    def setup(self):
        if os.getenv('EMBUCKET_ENABLED', '').lower() == 'true':
            self.embucket = EmbucketHelper(self.config)
            # Call setup_database_environment with connection settings
            catalog_url = f"{self.config['protocol']}://{self.config['host']}:{self.config['port']}"
            database = self.config.get('database', 'embucket')
            schema = self.config.get('schema', 'public')

            self.embucket.setup_database_environment(
                embucket_url=catalog_url,
                database=database,
                schema=schema,
            )

        # Create the database connection with proper configuration
        # The SQLLogicDatabase constructor will use embucket settings if present in config
        self.database = SQLLogicDatabase(':memory:', self.config, None)

        # Initialize the database only once for hot runs
        if self.config.get('reset_db', False):
            con = self.connection_pool.get_connection()
            reset_database(self.config, con)

    def resume_warehouse(self, disable_query_result_cache=False):
        """Resume the warehouse"""
        try:
            warehouse_name = self.config['warehouse']

            con = connect(
                user=self.config['user'],
                password=self.config['password'],
                account=self.config['account'],
                warehouse=warehouse_name
            )
            with con.cursor() as cursor:
                # Check warehouse state
                cursor.execute(f"SHOW WAREHOUSES LIKE '{warehouse_name}'")
                warehouse_info = cursor.fetchone()

                if warehouse_info:
                    state = warehouse_info[1]
                    if state == 'SUSPENDED':
                        start_time = time.time()
                        logger.info(f"Resuming warehouse '{warehouse_name}'...")
                        cursor.execute(f"ALTER WAREHOUSE {warehouse_name} RESUME")
                        elapsed_time = time.time() - start_time
                        logger.info(f"Warehouse '{warehouse_name}' resumed in {elapsed_time:.2f} seconds.")

                        # Set caching parameter based on run mode
                        if disable_query_result_cache:
                            cursor.execute(f"ALTER SESSION SET USE_CACHED_RESULT = FALSE")
                            logger.info("Query result cache disabled for cold run")
                        else:
                            cursor.execute(f"ALTER SESSION SET USE_CACHED_RESULT = TRUE")
                            logger.info("Query result cache enabled for hot run")
                    else:
                        logger.info(f"Warehouse '{warehouse_name}' is already active (state: {state}).")
                else:
                    logger.error(f"Warehouse '{warehouse_name}' not found.")
            con.close()
        except Exception as e:
            logger.error(f"Error resuming warehouse '{warehouse_name}': {e}")

    def execute_test(self, test: SQLLogicTest) -> ExecuteResult:
        self.reset()
        self.test = test
        self.original_sqlite_test = self.test.is_sqlite_test()

        # Top level keywords
        keywords = {
            '__TEST_DIR__': self.get_test_directory(),
            '__WORKING_DIRECTORY__': os.getcwd(),
            '__DATABASE__': self.config['database'],
            '__SCHEMA__': self.config['schema'],
        }

        def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
            # For cold runs, restart the warehouse before each query execution
            if self.run_mode == "cold":
                # Suspend and then resume to ensure fresh start with no cache
                self.suspend_warehouse()
                self.resume_warehouse(disable_query_result_cache=True)
            yield None

        self.database = SQLLogicDatabase(':memory:', self.config, None)
        pool = self.database.connect()

        # Initialize database if needed
        if self.config['reset_db']:
            con = pool.get_connection()
            reset_database(self.config, con)

        # Create context and execute test
        context = SQLLogicContext(pool, self, test.statements, keywords, update_value)
        context.is_loop = False
        pool.initialize_connection(context)

        context.verify_statements()
        res = context.execute()

        # Only clean up database for cold runs
        if self.run_mode == "cold" and self.database is not None:
            self.database.reset()
            # Final warehouse suspension for cold runs
            self.suspend_warehouse()

        # Clean up any databases that we created
        for loaded_path in self.loaded_databases:
            if not loaded_path:
                continue
            # Only delete database files that were created during the tests
            if not loaded_path.startswith(self.get_test_directory()):
                continue
            os.remove(loaded_path)

        return res

    def cleanup(self):
        """Clean up resources when done with all tests"""
        try:
            # Clean up hot mode resources
            if self.run_mode == "hot" and self.connection_pool is not None:
                if self.database is not None:
                    if self.config['reset_db']:
                        self.database.reset()
                    self.database = None

                # Close the connection pool - fixed method call
                if hasattr(self.connection_pool, 'close_all'):
                    self.connection_pool.close_all()
                elif hasattr(self.connection_pool, 'close'):
                    self.connection_pool.close()
                self.connection_pool = None

            # For cold runs, ensure warehouse is suspended at the end
            if self.run_mode == "cold":
                self.suspend_warehouse()

            # Clear any remaining loaded databases
            self.loaded_databases.clear()

            logger.info(f"Cleanup completed for {self.run_mode} run mode")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")


def directory_path_from_filename(path):
    return os.path.relpath(os.path.dirname(
        os.path.abspath(path)) # if no explicit dir in path
    )

import argparse


def load_config_from_env():
    """Load configuration from environment file."""
    load_dotenv()

    # Extract Snowflake configuration from environment variables
    config = {
        'account': os.getenv('SNOWFLAKE_ACCOUNT'),
        'user': os.getenv('SNOWFLAKE_USER'),
        'password': os.getenv('SNOWFLAKE_PASSWORD'),
        'warehouse': os.getenv('WAREHOUSE'),
        'database': os.getenv('DATABASE'),
        'schema': os.getenv('SCHEMA'),
        'reset_db': os.getenv('RESET_DB', 'false').lower() == 'true',
    }

    # Add embucket configuration if enabled via env
    if os.getenv('EMBUCKET_ENABLED', '').lower() == 'true':
        config.update({
            'protocol': os.getenv('EMBUCKET_PROTOCOL', 'http'),
            'host': os.getenv('EMBUCKET_HOST'),
            'port': os.getenv('EMBUCKET_PORT')
        })

    return config

class SQLLogicPythonRunner:
    def __init__(self, default_test_directory : Optional[str] = None):
        self.default_test_directory = default_test_directory

    def run(self):
        start_time = time.time()

        arg_parser = argparse.ArgumentParser(description='Execute SQL logic tests.')
        arg_parser.add_argument('--test-file', type=str, help='Path to the test file')
        arg_parser.add_argument('--test-dir', type=str, help='Path to the test directory holding the test files')
        arg_parser.add_argument('--benchmark', action='store_true',
                                help='Run in benchmark mode (compare hot/cold performance)')
        args = arg_parser.parse_args()

        # Load configuration from env file
        config = load_config_from_env()

        # Check if benchmark mode is enabled
        benchmark_mode = args.benchmark

        # Validation for benchmark mode
        if benchmark_mode and os.getenv('EMBUCKET_ENABLED', '').lower() == 'true':
            print("ERROR: Benchmark mode cannot be used with EMBUCKET_ENABLED=true")
            print("Please set EMBUCKET_ENABLED=false in .env file when using benchmark mode")
            return

        # Process test files
        file_paths = None

        # test_directory can be used for getting file_paths, or
        # if file_paths already provided, for some future purposes
        test_directory = None
        print(args.test_file)
        if args.test_file:
            file_paths = [args.test_file]
            test_directory = directory_path_from_filename(args.test_file)

        if not test_directory:
            if not args.test_dir:
                raise Exception("Did not provide a test directory, nor was a default directory set!")
            else:
                test_directory = args.test_dir

        if not file_paths:
            file_paths = glob.iglob(test_directory + '/**/*.slt', recursive=True)
            # use set as '**' with recursive true can return duplicate paths
            file_paths = list(set(os.path.relpath(path) for path in file_paths))

        print(f"tests directory: {test_directory} Test files: ", len(file_paths))

        total_tests = len(file_paths)
        if total_tests == 0:
            print('No tests located')
            exit(1)

        # If benchmark mode is enabled, run both hot and cold tests
        if benchmark_mode:
            # Run hot tests
            hot_start_time = time.time()
            print("\n=== Running HOT benchmark tests ===")
            self.run_tests(config, args, file_paths, test_directory, benchmark_mode, 'hot', hot_start_time)

            # Then cold tests first
            cold_start_time = time.time()
            print("\n=== Running COLD benchmark tests ===")
            self.run_tests(config, args, file_paths, test_directory, benchmark_mode, 'cold', cold_start_time)

            # Compare the results
            self.compare_benchmark_results()
        else:
            # Just run in standard mode (no benchmarking)
            default_run_mode = 'hot'  # Default to cold mode for regular runs
            print(f"\n=== Running SLT ===")
            self.run_tests(config, file_paths, test_directory, benchmark_mode, default_run_mode, start_time)

    def run_tests(self, config, file_paths, test_directory, benchmark_mode, run_mode, start_time):
        """Execute tests with the specified run mode"""

        executor = SQLLogicTestExecutor(config, test_directory, benchmark_mode, run_mode)
        executor.setup()

        # Error collection
        if benchmark_mode:
            error_log_file = f"{run_mode}_errors.log"  # File to save error logs
        else:
            error_log_file = "errors.log"
        error_details = defaultdict(list)  # Dictionary to hold errors grouped by category

        sql_parser = SQLLogicParser(None)
        all_results = []  # To collect statistics for all files
        execution_times_s = []  # Execution times in seconds for all queries in the order they were executed
        total_tests = 0  # Total number of tests across all files
        total_successful = 0  # Total number of successful tests across all files
        total_failed = 0  # Total number of failed tests across all files

        for i, file_path in enumerate(file_paths):
            if file_path in executor.SKIPPED_TESTS:
                continue

            print(f'[{i + 1}/{len(file_paths)}] {file_path}')
            try:
                test = sql_parser.parse(file_path)
            except SQLParserException as e:
                raise e
                executor.skip_log.append(str(e.message))
                continue

            skipped_statements_count = 0
            for st in test.statements:
                decorators = st.decorators
                if decorators:
                    if isinstance(decorators[0], SkipIf):
                        skip_decorator = decorators[0]
                        if skip_decorator.token.parameters[0] == 'always':
                            skipped_statements_count += 1

            if skipped_statements_count == len(test.statements):
                # Collect statistics for the file
                file_total_tests = skipped_statements_count
                file_successful_tests = 0
                file_failed_tests = 0
                file_success_percentage = 0
            else:
                # This is necessary to clean up databases/connections
                # So previously created databases are not still cached in the instance_cache
                gc.collect()
                result = executor.execute_test(test)
                queries_for_coverage = [q for q in result.queries if not q.exclude_from_coverage]
                # Collect statistics for the file
                file_total_tests = len(queries_for_coverage)  # Total tests in file
                execution_times_s += [query.execution_time_s for query in queries_for_coverage]
                file_successful_tests = sum(1 for query in queries_for_coverage if query.success)  # Successful tests
                file_failed_tests = file_total_tests - file_successful_tests  # Failed tests
                file_ran_tests = file_successful_tests + file_failed_tests
                file_success_percentage = (file_successful_tests / file_ran_tests) * 100 if file_ran_tests > 0 else 0

            # Update cumulative totals
            total_tests += file_total_tests
            total_successful += file_successful_tests
            total_failed += file_failed_tests

            page_name = extract_file_name(file_path)
            category = file_path.split('/')[-2]

            # Record the results for this file
            all_results.append({
                "category": category,
                "page_name": page_name,
                "total_tests": file_total_tests,
                "successful_tests": file_successful_tests,
                "failed_tests": file_failed_tests,
                "success_percentage": round(file_success_percentage, 2)
            })

            if skipped_statements_count != len(test.statements):
                for query in queries_for_coverage:
                    if not query.success:  # Process failed tests
                        error_message = query.error_message if hasattr(query, "error_message") else "Unknown Error"
                        error_details[page_name].append({
                            "error_message": error_message
                        })

        # Sort results by category
        sorted_results = sorted(all_results, key=lambda x: x["category"])

        display_page_results(sorted_results, total_tests, total_successful, total_failed)
        display_category_results(sorted_results)

        # Write results to CSV file
        csv_file_name = f"{run_mode}_test_statistics.csv" if benchmark_mode else "test_statistics.csv"
        with open(csv_file_name, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile,
                                    fieldnames=["page_name", "category", "total_tests", "successful_tests",
                                                "failed_tests",
                                                "success_percentage"])
            writer.writeheader()  # CSV header
            writer.writerows(all_results)  # Write each file's result

        if benchmark_mode:
            print(f"\nStatistics for {run_mode} mode tests have been saved to {csv_file_name}.")
        else:
            print(f"\nStatistics SLT have been saved to {csv_file_name}.")

        # Write error log to a file grouped by categories
        with open(error_log_file, "w") as error_file:
            for page_name, errors in error_details.items():
                error_file.write(f"Page name: {page_name}\n\n\n")
                for error in errors:
                    error_file.write(f"{error['error_message']}\n")

        # Calculate and display total execution time for benchmark mode
        if benchmark_mode:
            total_time = time.time() - start_time
            print(f"\nTotal execution time for {run_mode} run: {total_time:.2f} seconds")

            # Save timing information to CSV
            with open(f"{run_mode}_run_timing.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Run Mode", "Total Time (s)"])
                writer.writerow([run_mode, total_time])

            # Save per-query execution times to a separate CSV
            with open(f"{run_mode}_execution_times.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["#", "time(s)"])
                for i, time_s in enumerate(execution_times_s):
                    writer.writerow([i + 1, time_s])

        executor.cleanup()  # Clean up connection pool

        # Analyze performance for this run
        if benchmark_mode and not os.getenv('EMBUCKET_ENABLED', '').lower() == 'true':
            performance_stats = executor.analyze_query_performance(run_mode)
            if performance_stats:
                print(f"\n{run_mode.capitalize()} Run Query Performance:")
                print(f"Total queries analyzed: {performance_stats['count']}")
                print(f"Average compilation time: {performance_stats['avg_compilation_ms']:.2f} ms")
                print(f"Average execution time: {performance_stats['avg_execution_ms']:.2f} ms")
                print(f"Average total time: {performance_stats['avg_total_ms']:.2f} ms")

    def compare_benchmark_results(self):
        """Compare benchmark results between hot and cold runs"""
        print("\n=== BENCHMARK RESULTS ===")

        # Check if both run results exist
        if not (os.path.exists("cold_query_performance.csv") and os.path.exists("hot_query_performance.csv")):
            print("Missing performance data for comparison. Both hot and cold runs are required.")
            return

        print("\nPerformance Comparison (Cold vs Hot):")

        # Helper function to load stats from CSV
        def load_stats_from_csv(filename):
            avg_compilation = 0.0
            avg_execution = 0.0
            avg_total = 0.0
            count = 0

            with open(filename, "r") as csvfile:
                reader = csv.reader(csvfile)
                next(reader)  # Skip header
                rows = list(reader)
                count = len(rows)

                if count > 0:
                    avg_compilation = sum(float(row[2]) for row in rows) / count
                    avg_execution = sum(float(row[3]) for row in rows) / count
                    avg_total = sum(float(row[4]) for row in rows) / count

            return {
                'count': count,
                'avg_compilation_ms': avg_compilation,
                'avg_execution_ms': avg_execution,
                'avg_total_ms': avg_total
            }

        # Load stats from saved CSVs
        cold_stats = load_stats_from_csv("cold_query_performance.csv")
        hot_stats = load_stats_from_csv("hot_query_performance.csv")

        if cold_stats and hot_stats:
            # Calculate percentage improvements
            compilation_improvement = ((cold_stats['avg_compilation_ms'] - hot_stats['avg_compilation_ms']) /
                                       cold_stats['avg_compilation_ms'] * 100) if cold_stats[
                                                                                      'avg_compilation_ms'] > 0 else 0
            execution_improvement = ((cold_stats['avg_execution_ms'] - hot_stats['avg_execution_ms']) /
                                     cold_stats['avg_execution_ms'] * 100) if cold_stats['avg_execution_ms'] > 0 else 0
            total_improvement = ((cold_stats['avg_total_ms'] - hot_stats['avg_total_ms']) /
                                 cold_stats['avg_total_ms'] * 100) if cold_stats['avg_total_ms'] > 0 else 0

            # Display table with comparison
            comparison_table = PrettyTable()
            comparison_table.field_names = ["Metric", "Cold Run", "Hot Run", "Improvement"]

            comparison_table.add_row(["Query Count", cold_stats['count'], hot_stats['count'], "N/A"])
            comparison_table.add_row(["Avg. Compilation (ms)",
                                      f"{cold_stats['avg_compilation_ms']:.2f}",
                                      f"{hot_stats['avg_compilation_ms']:.2f}",
                                      f"{compilation_improvement:.2f}%"])
            comparison_table.add_row(["Avg. Execution (ms)",
                                      f"{cold_stats['avg_execution_ms']:.2f}",
                                      f"{hot_stats['avg_execution_ms']:.2f}",
                                      f"{execution_improvement:.2f}%"])
            comparison_table.add_row(["Avg. Total Time (ms)",
                                      f"{cold_stats['avg_total_ms']:.2f}",
                                      f"{hot_stats['avg_total_ms']:.2f}",
                                      f"{total_improvement:.2f}%"])

            print(comparison_table)

            # Write consolidated comparison to CSV
            with open("query_performance_comparison.csv", "w", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Metric", "Cold Run", "Hot Run", "Improvement (%)"])
                writer.writerow(["Query Count", cold_stats['count'], hot_stats['count'], "N/A"])
                writer.writerow(["Avg. Compilation (ms)",
                                 cold_stats['avg_compilation_ms'],
                                 hot_stats['avg_compilation_ms'],
                                 compilation_improvement])
                writer.writerow(["Avg. Execution (ms)",
                                 cold_stats['avg_execution_ms'],
                                 hot_stats['avg_execution_ms'],
                                 execution_improvement])
                writer.writerow(["Avg. Total Time (ms)",
                                 cold_stats['avg_total_ms'],
                                 hot_stats['avg_total_ms'],
                                 total_improvement])

        # Compare overall run times
        if os.path.exists("cold_run_timing.csv") and os.path.exists("hot_run_timing.csv"):
            cold_time = 0
            hot_time = 0

            with open("cold_run_timing.csv", "r") as cold_file:
                reader = csv.reader(cold_file)
                next(reader)  # Skip header
                for row in reader:
                    cold_time = float(row[1])

            with open("hot_run_timing.csv", "r") as hot_file:
                reader = csv.reader(hot_file)
                next(reader)  # Skip header
                for row in reader:
                    hot_time = float(row[1])

            if cold_time > 0 and hot_time > 0:
                speedup = (cold_time - hot_time) / cold_time * 100
                print(f"\nOverall Performance Comparison:")
                print(f"Cold run: {cold_time:.2f}s")
                print(f"Hot run: {hot_time:.2f}s")
                print(f"Speedup: {speedup:.2f}%")