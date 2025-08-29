import csv
import os
import glob
import json
from tabulate import tabulate
from collections import defaultdict


def print_test_failures():
    # Find all metrics files matching the pattern
    metrics_files = glob.glob("artifacts/metrics_*.csv")
    if not metrics_files:
        print("Error: No metrics files found in artifacts/")
        return

    csv_path = max(metrics_files, key=os.path.getmtime)
    print(f"Using most recent metrics file: {csv_path}")

    # Find corresponding mismatch file if it exists
    base_name = os.path.basename(csv_path)
    test_run_id = base_name.replace("metrics_", "").replace(".csv", "")
    mismatch_path = os.path.join("artifacts", f"mismatches_{test_run_id}.json")

    mismatches = {}
    if os.path.exists(mismatch_path):
        print(f"Found mismatch details file: {mismatch_path}")
        with open(mismatch_path) as f:
            mismatches = json.load(f)

    # Load and process the CSV
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames

        # Group failures by test (nodeid)
        failures = defaultdict(list)
        for row in reader:
            if row.get('passed', '').lower() == 'false':
                failures[row.get('nodeid', 'unknown')].append(row)

        if not failures:
            print("All tests passed!")
            return

        # Display summary
        print(f"Found {sum(len(rows) for rows in failures.values())} failures across {len(failures)} tests")

        # Display each test failure with details
        for test_id, rows in failures.items():
            print("\n" + "=" * 80)
            print(f"Test: {test_id}")
            print("-" * 80)

            # Display basic failure info
            table_rows = []
            for row in rows:
                table_rows.append([row.get(field, '') for field in headers])

            print(tabulate(table_rows, headers=headers, tablefmt="grid"))

            # Display detailed mismatch info if available
            for row in rows:
                query_id = row.get('query_id')
                if query_id in mismatches:
                    print(f"\nDetailed mismatches for query {query_id}:")

                    for mismatch in mismatches[query_id]:
                        if "type" in mismatch and mismatch["type"] == "row_count_mismatch":
                            print(
                                f"  Row count mismatch: Spark={mismatch['spark_rows']}, Embucket={mismatch['embucket_rows']}")
                        elif "total_mismatches" in mismatch:
                            print(f"  Total mismatches: {mismatch['total_mismatches']}")
                            print("  Sample mismatches:")

                            for detail in mismatch.get("details", []):
                                if detail["type"] == "row_structure_mismatch":
                                    print(f"    Row {detail['row_index']}: Structure mismatch - "
                                          f"Spark columns: {detail['spark_columns']}, "
                                          f"Embucket columns: {detail['embucket_columns']}")
                                elif detail["type"] == "data_mismatch":
                                    print(f"    Row {detail['row_index']}:")
                                    for col_idx, col_data in detail.get("columns", {}).items():
                                        print(f"      Column {col_idx}: "
                                              f"Spark='{col_data['spark_value']}', "
                                              f"Embucket='{col_data['embucket_value']}'")


if __name__ == "__main__":
    print_test_failures()