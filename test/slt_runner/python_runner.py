import json
import os
import glob
import time
from collections import defaultdict
from typing import Any, Generator, Optional, Dict
import gc
import csv
import multiprocessing

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

    def setup(self, is_embucket):
        if is_embucket:
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
            'embucket': True,
            'protocol': os.getenv('EMBUCKET_PROTOCOL', 'http'),
            'host': os.getenv('EMBUCKET_HOST'),
            'port': os.getenv('EMBUCKET_PORT'),
        })

    return config

class SQLLogicPythonRunner:
    def __init__(self, default_test_directory : Optional[str] = None):
        self.default_test_directory = default_test_directory

    def run_file(self, config, file_path, test_directory, benchmark_mode, run_mode):
        """Execute tests for a single file using a pre-created worker schema"""
        print(f"Processing file: {file_path}")

        executor = SQLLogicTestExecutor(config, test_directory, benchmark_mode, run_mode)

        results = []
        execution_times = []
        total = 0
        successful = 0
        failed = 0
        error_details = defaultdict(list)

        # Process the test file
        sql_parser = SQLLogicParser(None)
        try:
            test = sql_parser.parse(file_path)
            # Execute test and collect results
            result = executor.execute_test(test)

            # Calculate statistics
            queries_for_coverage = [q for q in result.queries if not q.exclude_from_coverage]
            file_total = len(queries_for_coverage)
            file_successful = sum(1 for q in queries_for_coverage if q.success)
            file_failed = file_total - file_successful

            # Update counters
            total += file_total
            successful += file_successful
            failed += file_failed
            execution_times += [q.execution_time_s for q in queries_for_coverage]

            page_name = extract_file_name(file_path)
            results.append({
                "category": os.path.dirname(file_path).split('/')[-1],
                "page_name": page_name,
                "total_tests": file_total,
                "successful_tests": file_successful,
                "failed_tests": file_failed,
                "success_percentage": (file_successful / file_total * 100) if file_total > 0 else 0
            })

            # Collect errors
            for query in result.queries:
                if not query.success and query.error_message:
                    error_details[os.path.dirname(file_path).split('/')[-1]].append(
                        f"{page_name}: {query.query}\n{query.error_message}\n")

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            error_details[os.path.dirname(file_path).split('/')[-1]].append(f"{file_path}: {str(e)}")

        executor.cleanup()

        return {
            "category": os.path.dirname(file_path).split('/')[-1],
            "results": results,
            "execution_times": execution_times,
            "total": total,
            "successful": successful,
            "failed": failed,
            "error_details": error_details
        }

    def run_files_parallel(self, config, file_paths, test_directory, benchmark_mode, run_mode, start_time, is_embucket,
                           max_workers=None):
        """Execute test files in parallel with pre-created worker schemas"""
        import concurrent.futures
        print(f"\n=== Running {len(file_paths)} files in parallel with {max_workers} workers ===")

        executor = SQLLogicTestExecutor(config, test_directory, benchmark_mode, run_mode)
        executor.setup(is_embucket)

        worker_schemas = []

        # Handle Embucket setup separately from Snowflake
        if is_embucket:
            try:
                embucket_url = f"{config.get('protocol', 'http')}://{config.get('host', 'localhost')}:{config.get('port', '3000')}"
                headers = {'Content-Type': 'application/json'}
                database = config.get('database', 'embucket')
                schema = config.get('schema', 'public')

                # Create unique schemas for each worker in Embucket
                print('Creating worker schemas for Embucket:')
                for worker_id in range(1, max_workers + 1):
                    worker_schema = f"{schema}_{worker_id}"
                    worker_schemas.append(worker_schema)
                    try:
                        # Create schema using Embucket API
                        query = f"CREATE SCHEMA IF NOT EXISTS {database}.{worker_schema}"
                        requests.request(
                            "POST", f"{embucket_url}/ui/queries",
                            headers=headers,
                            data=json.dumps({"query": query})
                        ).json()
                        print(f"Created worker schema for Embucket: {worker_schema}")
                    except Exception as e:
                        print(f"Error creating Embucket worker schema {worker_schema}: {e}")
                        # Fall back to base schema if creation fails
                        worker_schemas[-1] = schema

            except Exception as e:
                print(f"Error setting up Embucket schema: {e}")
                # Fall back to default schema in case of error
                worker_schemas = [config['schema']] * max_workers
        else:
            # Only create Snowflake schemas if not using Embucket
            try:
                # Connect to the database
                con = connect(
                    user=config['user'],
                    password=config['password'],
                    account=config['account'],
                    warehouse=config['warehouse'],
                    database=config['database']
                )

                # Create schemas for each worker in Snowflake
                print('Creating worker schemas for Snowflake:')
                with con.cursor() as cursor:
                    for worker_id in range(1, max_workers + 1):
                        worker_schema = f"{config['schema']}_{worker_id}"
                        worker_schemas.append(worker_schema)
                        try:
                            # Create a unique schema for this worker
                            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {worker_schema}")
                            print(f"Created worker schema for Snowflake: {worker_schema}")
                        except Exception as e:
                            print(f"Error creating worker schema {worker_schema}: {e}")
                con.close()
            except Exception as e:
                print(f"Error connecting to database to create schemas: {e}")
                # Fall back to original schema if schema creation fails
                worker_schemas = [config['schema']] * max_workers

        # Ensure worker_schemas is not empty to prevent division by zero
        if not worker_schemas:
            worker_schemas = [config['schema']] * max_workers
            print("No worker schemas created, using default schema")

        # Execute files in parallel
        all_results = []
        all_execution_times = []
        total_tests = 0
        total_successful = 0
        total_failed = 0
        all_error_details = defaultdict(list)

        # Create worker-specific configs
        worker_configs = []
        for schema in worker_schemas:
            worker_config = config.copy()
            worker_config['schema'] = schema
            worker_configs.append(worker_config)

        # Create a pool of workers with assigned configs
        with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Distribute files among workers
            future_to_file = {}
            for i, file_path in enumerate(file_paths):
                worker_index = i % len(worker_configs)
                future_to_file[executor.submit(
                    self.run_file,
                    worker_configs[worker_index],
                    file_path,
                    test_directory,
                    benchmark_mode,
                    run_mode
                )] = file_path

            for future in concurrent.futures.as_completed(future_to_file):
                result = future.result()
                all_results.extend(result["results"])
                all_execution_times.extend(result["execution_times"])
                total_tests += result["total"]
                total_successful += result["successful"]
                total_failed += result["failed"]

                # Collect errors
                for cat, errors in result["error_details"].items():
                    all_error_details[cat].extend(errors)

            # Clean up worker schemas after all tests are done
            if is_embucket:
                try:
                    # Setup Embucket API URL for cleanup
                    embucket_url = f"{config.get('protocol', 'http')}://{config.get('host', 'localhost')}:{config.get('port', '3000')}"
                    headers = {'Content-Type': 'application/json'}
                    database = config.get('database', 'embucket')

                    # clean up any temporary objects created
                    print("Cleaning up Embucket workers data:")

                    # Use Embucket API to run cleanup queries as needed
                    for schema in set(worker_schemas):
                        if schema == config['schema']:  # Skip primary schema if it matches default
                            continue

                        try:
                            # Drop the entire worker schema instead of individual tables
                            drop_query = f"DROP SCHEMA IF EXISTS {database}.{schema} CASCADE"
                            requests.request(
                                "POST", f"{embucket_url}/ui/queries",
                                headers=headers,
                                data=json.dumps({"query": drop_query})
                            )
                            print(f"Dropped schema {schema} in Embucket")
                        except Exception as e:
                            print(f"Error cleaning up Embucket schema {schema}: {e}")
                except Exception as e:
                    print(f"Error in Embucket cleanup: {e}")
            else:
                # Original Snowflake cleanup logic for non-Embucket runs
                # clean up any temporary objects created
                print("Cleaning up Snowflake workers data:")
                try:
                    con = connect(
                        user=config['user'],
                        password=config['password'],
                        account=config['account'],
                        warehouse=config['warehouse'],
                        database=config['database']
                    )
                    with con.cursor() as cursor:
                        for schema in worker_schemas:
                            if schema == config['schema']:  # Skip dropping the default schema
                                continue

                            # First check if the TEST_USER exists
                            cursor.execute("SHOW USERS LIKE 'TEST_USER'")
                            user_exists = cursor.fetchone() is not None

                            if user_exists:
                                # Check for any policies set on TEST_USER
                                try:
                                    cursor.execute(f"""
                                        SELECT policy_name, policy_schema
                                        FROM TABLE(
                                            INFORMATION_SCHEMA.POLICY_REFERENCES(
                                                REF_ENTITY_NAME => 'test_user',
                                                REF_ENTITY_DOMAIN => 'USER'
                                            )
                                        )
                                    """)
                                    policies = cursor.fetchall()

                                    # Unset each policy before dropping the schema
                                    for policy in policies:
                                        try:
                                            cursor.execute(
                                                f"ALTER USER TEST_USER UNSET AUTHENTICATION POLICY")
                                        except Exception as policy_error:
                                            print(
                                                f"Error unsetting policy: {policy_error}")
                                except Exception as ref_error:
                                    print(f"Error checking policy references: {ref_error}")

                            # Now drop the schema
                            cursor.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
                            print(f"Cleaned up worker schema {schema}")
                    con.close()
                except Exception as e:
                    print(f"Error cleaning up worker schemas: {e}")

            # Display and save results
            display_page_results(all_results, total_tests, total_successful, total_failed)
            display_category_results(all_results)

        # Save results to CSV
        csv_file_name = f"{run_mode}_test_statistics.csv" if benchmark_mode else "test_statistics.csv"
        with open(csv_file_name, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile,
                                    fieldnames=["page_name", "category", "total_tests", "successful_tests",
                                                "failed_tests", "success_percentage"])
            writer.writeheader()
            writer.writerows(all_results)

        # Write error log
        error_log_file = f"{run_mode}_errors.log" if benchmark_mode else "errors.log"
        with open(error_log_file, "w") as error_file:
            for category, errors in all_error_details.items():
                if errors:
                    for error in errors:
                        error_file.write(f"{error}\n")

        # Log execution time
        total_time = time.time() - start_time
        print(f"\nTotal execution time: {total_time:.2f} seconds")

    def run(self):
        start_time = time.time()

        # Get system CPU count for intelligent defaults
        cpu_count = multiprocessing.cpu_count()

        # Use a parser that tracks which arguments were explicitly provided
        arg_parser = argparse.ArgumentParser(description='Execute SQL logic tests.')
        arg_parser.add_argument('--test-file', type=str, help='Path to the test file')
        arg_parser.add_argument('--test-dir', type=str, help='Path to the test directory holding the test files')
        arg_parser.add_argument('--benchmark', action='store_true',
                                help='Run in benchmark mode (compare hot/cold performance)')
        arg_parser.add_argument('--parallel', action='store_true',
                                help='Run files in parallel')
        arg_parser.add_argument('--workers', type=int, default=None,
                                help=f'Number of parallel workers (default: {cpu_count}, system CPU count)')

        args = arg_parser.parse_args()

        workers = 1  # Default to 1 worker if not parallel
        if args.parallel:
            if args.workers is None:
                workers = cpu_count
                print(f"Using default worker count: {workers} (system CPU count)")
            else:
                workers = args.workers
                if workers <= 0:
                    print(f"Invalid worker count ({workers}). Using CPU count ({cpu_count}) instead.")
                    workers = cpu_count
                elif workers > cpu_count:
                    print(
                        f"Specified worker count ({workers}) exceeds CPU count ({cpu_count}). Using CPU count instead.")
                    workers = cpu_count
        else:
            print("Running in non-parallel mode with 1 worker.")

        # Load configuration from env file
        config = load_config_from_env()
        # Check if Embucket is enabled
        is_embucket = config.get('embucket')

        # check if Embucket is running:
        if is_embucket:
            try:
                embucket_url = f"{config.get('protocol', 'http')}://{config.get('host', 'localhost')}:{config.get('port', '3000')}/health"
                response = requests.get(embucket_url, timeout=5)
                if response.status_code != 200:
                    print(f"ERROR: Embucket is enabled but not responding correctly: {response}")
                    return
            except requests.exceptions.RequestException:
                print("ERROR: Cannot connect to Embucket. Please make sure the service is running.")
                return

        # Check if benchmark mode is enabled
        benchmark_mode = args.benchmark

        # Validation for benchmark mode
        if benchmark_mode and is_embucket:
            print("ERROR: Benchmark mode cannot be used with EMBUCKET_ENABLED=true")
            print("Please set EMBUCKET_ENABLED=false in .env file when using benchmark mode")
            return

        # Early check for Embucket connectivity if enabled
        if os.getenv('EMBUCKET_ENABLED', '').lower() == 'true':
            try:
                # Verify Embucket server is available
                catalog_url = f"{config['protocol']}://{config['host']}:{config['port']}"
                headers = {'Content-Type': 'application/json'}

                try:
                    response = requests.get(catalog_url, headers=headers, timeout=5)
                    if response.status_code != 200:
                        print(f"ERROR: Embucket server returned status {response.status_code}")
                        print("Please ensure Embucket server is running correctly")
                        return
                except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                    print(f"ERROR: Could not connect to Embucket server: {e}")
                    print("Please ensure Embucket server is running and accessible")
                    return

            except Exception as e:
                print(f"ERROR: Embucket configuration error: {e}")
                return

        # Process test files
        file_paths = None

        # test_directory can be used for getting file_paths, or
        # if file_paths already provided, for some future purposes
        test_directory = None
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

        print(f"Tests directory: {test_directory} Test files: {len(file_paths)}")

        total_tests = len(file_paths)
        if total_tests == 0:
            print('No tests located')
            exit(1)

        # If benchmark mode is enabled, run both hot and cold tests
        if benchmark_mode:
            self.compare_benchmark_results()
        else:
            # Just run in standard mode (no benchmarking)
            default_run_mode = 'hot'  # Default to hot mode for regular runs
            print(f"\n=== Running SLT ===")
            self.run_files_parallel(config, file_paths, test_directory, benchmark_mode, default_run_mode, start_time, is_embucket,
                                    workers)

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