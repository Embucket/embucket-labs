import pandas as pd
import glob
import re
import os

from constants import SystemType

COLUMN_TO_AVERAGE = 'Total (ms)'


def sort_by_query_index(df):
    """Add index column to dataframe based on query number.
    If df['Query'] contains ['tpch-q20', 'tpch-q3'],
    the function extracts the numbers (20, 3) and sorts accordingly
    """
    # Add index column
    df['query_index'] = df['Query'].apply(
        lambda x: int(re.search(r'q(\d+)', x).group(1))
        if re.search(r'q(\d+)', x) else float('inf')
    )
    # Sort by the new index
    df = df.sort_values('query_index')

    # Remove the index column
    df = df.drop('query_index', axis=1)
    return df


def calculate_benchmark_averages(schema, warehouse, system, benchmark_type):
    """
    Calculate average results for benchmark runs.
    Args:
        schema: The schema/dataset used in the benchmark
        warehouse: The warehouse/instance used in the benchmark
        system: system like 'embucket','snowflake',
    """
    if system == SystemType.EMBUCKET:
        search_dir = f'result/embucket_{benchmark_type}_results/{schema}/{warehouse}'
    elif system == SystemType.SNOWFLAKE:
        search_dir = f'result/snowflake_{benchmark_type}_results/{schema}/{warehouse}'
    else:
        raise ValueError("Unsupported system")

    # Get all CSV files from the specified directory
    all_csv_files = glob.glob(os.path.join(search_dir, '*.csv'))

    # Skip if no files found
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

        # Sort each DataFrame by 'Query' to align rows
        dfs = [df.sort_values('Query').reset_index(drop=True) for df in dfs]

        # Concatenate DataFrames along a new axis
        stacked = pd.concat(dfs, axis=0, keys=range(len(dfs)))

        # Stack and average
        dfs = [df.sort_values('Query').reset_index(drop=True) for df in dfs]
        stacked = pd.concat(dfs, axis=0, keys=range(len(dfs)))
        averaged = stacked.groupby(level=1)[[COLUMN_TO_AVERAGE]].mean().reset_index(drop=True)
        averaged['Query'] = dfs[0]['Query']
        averaged = averaged[['Query', COLUMN_TO_AVERAGE]]
        averaged = sort_by_query_index(averaged)

        # Move TOTAL row to the top if it exists
        if 'TOTAL' in averaged['Query'].values:
            total_row = averaged[averaged['Query'] == 'TOTAL']
            other_rows = averaged[averaged['Query'] != 'TOTAL']
            averaged = pd.concat([total_row, other_rows]).reset_index(drop=True)

        # Save to CSV
        output_filename = os.path.join(search_dir, f'avg_results.csv')
        averaged.to_csv(output_filename, index=False)
        print(f"Created average file: {output_filename}")
