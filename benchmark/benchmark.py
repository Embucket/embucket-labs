import glob
import os

from calculate_average import calculate_benchmark_averages
from utils import create_snowflake_connection
from utils import create_embucket_connection
from tpch_queries import TPCH_QUERIES

from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
import csv

load_dotenv()


def save_results_to_csv(results, is_embucket=True, filename="query_results.csv"):
    """Save benchmark results to CSV file."""
    if is_embucket:
        query_results, total_time = results
        headers = ["Query", "Query ID", "Total (ms)", "Rows"]

        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for row in query_results:
                writer.writerow([row[0], row[1], row[2], row[3]])  # query, query_id, query_time, rows
            writer.writerow(["TOTAL", "", total_time, ""])
    else:
        headers = ["Query", "Query ID", "Compilation (ms)", "Execution (ms)", "Total (ms)", "Rows"]

        # Calculate totals
        total_compilation_time = sum(row[2] for row in results)
        total_execution_time = sum(row[3] for row in results)
        total_time = total_compilation_time + total_execution_time

        with open(filename, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(results)
            writer.writerow(["TOTAL", "", total_compilation_time, total_execution_time, total_time, ""])


def display_comparison(sf_results, emb_results):
    """Display graphical comparison of query times between Snowflake and Embucket."""

    # Process Snowflake results
    sf_query_times = {}
    for row in sf_results:
        query_number = row[0]
        total_time = row[4]  # Total time column in Snowflake results
        sf_query_times[query_number] = total_time

    # Process Embucket results
    emb_query_times = {}
    query_results, _ = emb_results
    for row in query_results:
        query_number = row[0]
        query_time = row[2]  # Query time column in Embucket results
        emb_query_times[query_number] = query_time

    queries_to_compare = sorted(set(sf_query_times.keys()).intersection(set(emb_query_times.keys())))

    # Prepare data for plotting
    queries = [str(q) for q in queries_to_compare]
    sf_times = [sf_query_times.get(q, 0) for q in queries_to_compare]
    emb_times = [emb_query_times.get(q, 0) for q in queries_to_compare]

    # Create bar chart
    x = np.arange(len(queries))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 8))
    sf_bars = ax.bar(x - width / 2, sf_times, width, label='Snowflake')
    emb_bars = ax.bar(x + width / 2, emb_times, width, label='Embucket')

    # Add labels and title
    ax.set_xlabel('Query Number')
    ax.set_ylabel('Execution Time (ms)')
    ax.set_title('Query Execution Time Comparison: Snowflake vs. Embucket')
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()

    # Add ratio table below the chart
    ratios = [f"{emb / sf:.2f}x" if sf > 0 else "N/A" for sf, emb in zip(sf_times, emb_times)]
    table_data = [[f"{sf:.2f}", f"{emb:.2f}", ratio] for sf, emb, ratio in zip(sf_times, emb_times, ratios)]

    the_table = plt.table(
        cellText=table_data,
        rowLabels=queries,
        colLabels=["Snowflake (ms)", "Embucket (ms)", "Ratio (Emb/SF)"],
        loc='bottom',
        bbox=[0.0, -0.50, 1.0, 0.3]
    )

    # Adjust layout for table
    plt.subplots_adjust(bottom=0.3)
    plt.tight_layout()
    plt.savefig("query_comparison.png")
    plt.show()

    # Print summary
    sf_total = sum(sf_times)
    emb_total = sum(emb_times)
    print(f"\nTotal Time Comparison:")
    print(f"Snowflake: {sf_total:.2f} ms")
    print(f"Embucket: {emb_total:.2f} ms")
    print(f"Ratio (Embucket/Snowflake): {emb_total / sf_total:.2f}x" if sf_total > 0 else "N/A")


def run_on_sf(cursor, sf_warehouse):
    """Run TPCDS queries on Snowflake and measure performance."""
    executed_query_ids = []
    query_id_to_number = {}
    results = []

    # Execute queries
    for query_number, query in TPCH_QUERIES:
        try:
            print(f"Executing query {query_number}...")

            # Suspend warehouse before each query to ensure clean state
            if sf_warehouse:
                try:
                    cursor.execute(f"ALTER WAREHOUSE {sf_warehouse} SUSPEND;")
                    cursor.execute("SELECT SYSTEM$WAIT(2);")
                    cursor.execute(f"ALTER WAREHOUSE {sf_warehouse} RESUME;")
                except Exception as e:
                    print(f"Warning: Could not suspend/resume warehouse for query {query_number}: {e}")

            # Execute the actual query
            cursor.execute(query)
            _ = cursor.fetchall()
            cursor.execute("SELECT LAST_QUERY_ID()")
            query_id = cursor.fetchone()[0]
            if query_id:
                executed_query_ids.append(query_id)
                query_id_to_number[query_id] = query_number
        except Exception as e:
            print(f"Error executing query {query_number}: {e}")

    # Collect performance metrics
    if executed_query_ids:
        query_ids_str = "', '".join(executed_query_ids)
        cursor.execute(f"""
            SELECT
                QUERY_ID,
                COMPILATION_TIME,
                EXECUTION_TIME,
                TOTAL_ELAPSED_TIME,
                ROWS_PRODUCED
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 1000))
            WHERE QUERY_ID IN ('{query_ids_str}')
            ORDER BY START_TIME
            """)

        query_history = cursor.fetchall()

        # Format results as [query_number, query_id, compilation_time, execution_time, total_time, rows]
        for record in query_history:
            query_id = record[0]
            compilation_time = record[1]
            execution_time = record[2]
            total_time = record[3]
            rows = record[4]
            query_number = query_id_to_number.get(query_id)

            if query_number:
                results.append([
                    query_number,
                    query_id,
                    compilation_time,
                    execution_time,
                    total_time,
                    rows
                ])

    return results


def run_on_emb(cursor):
    """Run TPCDS queries on Embucket and measure performance."""
    executed_query_ids = []
    query_id_to_number = {}

    for query_number, query in TPCH_QUERIES:
        try:
            print(f"Executing query {query_number}...")
            cursor.execute(query)
            _ = cursor.fetchall()  # Fetch results but don't store them

            # Get query ID
            cursor.execute("SELECT LAST_QUERY_ID()")
            query_id = cursor.fetchone()[0]

            if query_id:
                executed_query_ids.append(query_id)
                query_id_to_number[query_id] = query_number
        except Exception as e:
            print(f"Error executing query {query_number}: {e}")

    # Retrieve query history data from Embucket
    query_results = []
    total_time = 0

    if executed_query_ids:
        query_ids_str = "', '".join(executed_query_ids)
        history_query = f"SELECT id, Duration_ms, Result_count FROM slatedb.history.queries WHERE id IN ('{query_ids_str}')"

        try:
            cursor.execute(history_query)
            history_results = cursor.fetchall()

            # Format the results and calculate total time
            for record in history_results:
                query_id = record[0]
                duration_ms = record[1]
                result_count = record[2]
                query_number = query_id_to_number.get(str(query_id))

                # Add to total time
                total_time += duration_ms

                if query_number:
                    query_results.append([
                        query_number,
                        query_id,
                        duration_ms,
                        result_count
                    ])
        except Exception as e:
            print(f"Error retrieving query history: {e}")

    return query_results, total_time


def run_snowflake_benchmark(run_number):
    """Run benchmark on Snowflake."""
    # Run Snowflake benchmark
    sf_connection = create_snowflake_connection()
    sf_warehouse = sf_connection.warehouse
    sf_schema = sf_connection.schema
    sf_cursor = sf_connection.cursor()

    # Disable query result caching for benchmark
    sf_cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")

    sf_results = run_on_sf(sf_cursor, sf_warehouse)
    output_path = f"snowflake_tpch_results/{sf_schema}/{sf_warehouse}/snowflake_results_run_{run_number}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    save_results_to_csv(sf_results, is_embucket=False, filename=output_path)

    sf_cursor.close()
    sf_connection.close()

    # Check if we have 5 CSV files ready and calculate averages if so
    search_dir = f"snowflake_tpch_results/{sf_schema}/{sf_warehouse}"
    csv_files = glob.glob(os.path.join(search_dir, "snowflake_results_run_*.csv"))
    if len(csv_files) == 5:
        print(f"Found 5 CSV files. Calculating averages...")
        calculate_benchmark_averages(sf_schema, sf_warehouse, is_embucket=False)

    return sf_results


def run_embucket_benchmark(run_number):
    """Run benchmark on Embucket."""
    embucket_instance = os.getenv("EMBUCKET_INSTANCE")
    embucket_dataset = os.getenv("EMBUCKET_DATASET")
    embucket_connection = create_embucket_connection()
    embucket_cursor = embucket_connection.cursor()

    emb_results = run_on_emb(embucket_cursor)
    emb_output_path = f"embucket_tpch_results/{embucket_dataset}/{embucket_instance}/embucket_results_run_{embucket_instance}_{run_number}.csv"
    os.makedirs(os.path.dirname(emb_output_path), exist_ok=True)
    save_results_to_csv(emb_results, is_embucket=True, filename=emb_output_path)

    embucket_cursor.close()
    embucket_connection.close()

    # Check if we have 5 CSV files ready and calculate averages if so
    search_dir = f"embucket_tpch_results/{embucket_dataset}/{embucket_instance}"
    csv_files = glob.glob(os.path.join(search_dir, "embucket_*.csv"))
    if len(csv_files) == 5:
        print(f"Found 5 CSV files. Calculating averages...")
        calculate_benchmark_averages(embucket_dataset, embucket_instance, is_embucket=True)
    return emb_results


def run_benchmark(run_number):
    """Main function to run benchmarks on both platforms."""
    emb_results = run_embucket_benchmark(run_number)
    sf_results = run_snowflake_benchmark(run_number)

    # Display comparison only if Snowflake results exist
    if sf_results:
        display_comparison(sf_results, emb_results)
    else:
        print("Skipping comparison as Snowflake benchmark was not run")


if __name__ == "__main__":
    for i in range(5):
        print(f"Run {i + 1} of 5")
        run_benchmark(i + 1)