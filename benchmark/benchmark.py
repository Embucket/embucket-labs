import csv
from snowflake.connector import connect
from tabulate import tabulate
from config import get_snowflake_config
from tpcds_queries import TPCDS_QUERIES


def run_on_sf(cursor):
    """Run TPCDS queries on Snowflake and measure performance."""
    executed_query_ids = []
    query_id_to_number = {}
    all_results = []

    # Execute queries
    for query_number, query in TPCDS_QUERIES:
        try:
            print(f"Executing query {query_number}...")
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
            FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
            WHERE QUERY_ID IN ('{query_ids_str}')
            ORDER BY START_TIME
            """)

        results = cursor.fetchall()
        total_compilation_time = 0
        total_execution_time = 0

        table_data = []
        headers = ["Query", "Query ID", "Compilation (ms)", "Execution (ms)", "Total (ms)", "Rows"]

        for row in results:
            query_id = row[0]
            query_number = query_id_to_number.get(query_id, "")
            compilation_time = float(row[1]) if row[1] is not None else 0.0
            execution_time = float(row[2]) if row[2] is not None else 0.0
            total_time = float(row[3]) if row[3] is not None else 0.0
            rows = row[4] if row[4] is not None else 0

            total_compilation_time += compilation_time
            total_execution_time += execution_time

            all_results.append([
                query_number,  # Query number
                query_id,  # Query ID
                compilation_time,  # Compilation (ms)
                execution_time,  # Execution (ms)
                total_time,  # Total (ms)
                rows,  # Rows
            ])

            table_data.append([
                query_number,
                query_id,
                compilation_time,
                execution_time,
                total_time,
                rows
            ])

        # Add totals row for display
        table_data.append([
            "TOTAL",
            "",
            total_compilation_time,
            total_execution_time,
            total_compilation_time + total_execution_time,
            ""
        ])

        # Save results to CSV
        with open("query_results.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(all_results)
            # Add totals row
            writer.writerow(["TOTAL", "", total_compilation_time, total_execution_time,
                             total_compilation_time + total_execution_time, ""])

        # Print results using tabulate
        print("\nBenchmark Results:")
        print("------------------")
        print(tabulate(table_data, headers=headers, tablefmt="grid", floatfmt=".2f"))
        print(f"\nTotal Compilation Time: {total_compilation_time:.2f} ms")
        print(f"Total Execution Time: {total_execution_time:.2f} ms")
        print(f"Total Time: {(total_compilation_time + total_execution_time):.2f} ms")
    else:
        print("No queries were executed successfully.")


def run_benchmark():
    """Main function to run the benchmark."""
    sf_config = get_snowflake_config()
    sf_connection = connect(**sf_config)
    cursor = sf_connection.cursor()

    # Disable query result caching for benchmark
    cursor.execute("ALTER SESSION SET USE_CACHED_RESULT = FALSE;")

    run_on_sf(cursor)

    cursor.close()
    sf_connection.close()


if __name__ == "__main__":
    run_benchmark()