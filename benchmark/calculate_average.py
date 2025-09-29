import pandas as pd
import glob
import re
import os

from constants import SystemType

COLUMN_TO_AVERAGE = 'Total (ms)'


def sort_by_query_index(df):
    """Sort dataframe by query number.
    Handles numeric queries like '1', '2', '10' and puts 'TOTAL' at the end.
    """
    def extract_query_number(query_name):
        # Handle TOTAL row - should be last
        if str(query_name).upper() == 'TOTAL':
            return float('inf')

        # Parse as plain number
        try:
            return int(query_name)
        except (ValueError, TypeError):
            return float('inf')

    # Add index column
    df['query_index'] = df['Query'].apply(extract_query_number)
    # Sort by the new index
    df = df.sort_values('query_index')

    # Remove the index column
    df = df.drop('query_index', axis=1)
    return df


def calculate_benchmark_averages(dataset, warehouse, system, benchmark_type, cached=False, disable_result_cache=False):
    """
    Calculate average results for benchmark runs.
    Args:
        dataset: The dataset used in the benchmark
        warehouse: The warehouse/instance used in the benchmark
        system: system like 'embucket','snowflake'
        benchmark_type: The benchmark type ('tpch', 'tpcds')
        cached: Whether these are cached results (True) or no-cache results (False)
    """
    run_type = "warm" if cached else "cold"

    if system == SystemType.SNOWFLAKE and disable_result_cache:
        cache_folder = f"{run_type}_no_result_cache"
    else:
        cache_folder = run_type

    if system == SystemType.EMBUCKET:
        search_dir = f'result/embucket_{benchmark_type}_results/{dataset}/{warehouse}/{cache_folder}'
    elif system == SystemType.SNOWFLAKE:
        search_dir = f'result/snowflake_{benchmark_type}_results/{dataset}/{warehouse}/{cache_folder}'
    else:
        raise ValueError("Unsupported system")

    all_csv_files = glob.glob(os.path.join(search_dir, '*.csv'))

    if not all_csv_files:
        print(f"No CSV files found in {search_dir}")
        return

    print(f"Found {len(all_csv_files)} CSV files in {search_dir}")

    # Group files by pattern type
    instance_files = {}
    for file in all_csv_files:
        # Extract pattern type using regex
        match = re.search(r'(embucket|snowflake)_results(?:_run_(\d+))?', os.path.basename(file))
        if match:
            pattern_type = match.group(1)  # embucket or snowflake
            if pattern_type not in instance_files:
                instance_files[pattern_type] = []
            instance_files[pattern_type].append(file)

    for pattern_type, csv_files in instance_files.items():
        # Sort the file paths for consistent processing
        csv_files.sort()

        print(f"Processing {pattern_type} with {len(csv_files)} files")

        if len(csv_files) < 1:
            print(f"No files found for averaging (found {len(csv_files)})")
            continue

        # Read all DataFrames
        dfs = [pd.read_csv(f) for f in csv_files]
        print(f"Using all {len(dfs)} files for averaging: {csv_files}")

        # Stack and average
        dfs = [df.sort_values('Query').reset_index(drop=True) for df in dfs]
        stacked = pd.concat(dfs, axis=0, keys=range(len(dfs)))
        averaged = stacked.groupby(level=1)[[COLUMN_TO_AVERAGE]].mean().reset_index(drop=True)
        averaged['Query'] = dfs[0]['Query']
        averaged = averaged[['Query', COLUMN_TO_AVERAGE]]
        averaged = sort_by_query_index(averaged)

        # Save to CSV
        output_filename = os.path.join(search_dir, f'avg_results.csv')
        averaged.to_csv(output_filename, index=False)
        print(f"Created average file: {output_filename}")
