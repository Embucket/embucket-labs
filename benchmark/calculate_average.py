import pandas as pd
import glob
import re
import os


def natural_sort_queries(df):
    """Sort queries numerically."""
    df['sort_key'] = df['Query'].apply(lambda x:
                                       int(re.search(r'q(\d+)', x).group(1))
                                       if re.search(r'q(\d+)', x) else 0)
    df = df.sort_values('sort_key')
    df = df.drop('sort_key', axis=1)
    return df


def calculate_benchmark_averages(schema, warehouse, is_embucket=False):
    """
    Calculate average results for benchmark runs, excluding min and max runs.

    Args:
        schema: The schema/dataset used in the benchmark
        warehouse: The warehouse/instance used in the benchmark
        is_embucket: Flag to indicate if processing Embucket results (True) or Snowflake results (False)
    """
    # Directory where benchmark results are stored
    if is_embucket:
        search_dir = f'embucket_tpch_results/{schema}/{warehouse}'
    else:
        search_dir = f'snowflake_tpch_results/{schema}/{warehouse}'

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

        if len(csv_files) < 3:
            print(f"Not enough files to remove min/max (found {len(csv_files)})")
            continue

        # Read all DataFrames
        all_dfs = [pd.read_csv(f) for f in csv_files]

        # Extract TOTAL values to identify min and max files
        total_values = []
        for i, df in enumerate(all_dfs):
            # Check if TOTAL row exists
            total_row = df[df['Query'] == 'TOTAL']
            if not total_row.empty and 'Total (ms)' in df.columns:
                total_value = total_row['Total (ms)'].values[0]
                total_values.append((i, total_value))

        # Sort by total values
        total_values.sort(key=lambda x: x[1])

        # Get indices of min and max files
        min_idx = total_values[0][0]
        max_idx = total_values[-1][0]

        print(f"Removing file with lowest TOTAL: {csv_files[min_idx]} (value: {total_values[0][1]})")
        print(f"Removing file with highest TOTAL: {csv_files[max_idx]} (value: {total_values[-1][1]})")

        # Filter out min and max files
        filtered_indices = [i for i in range(len(all_dfs)) if i != min_idx and i != max_idx]
        dfs = [all_dfs[i] for i in filtered_indices]
        used_files = [csv_files[i] for i in filtered_indices]
        print(f"Using {len(dfs)} files for averaging: {used_files}")

        # Sort each DataFrame by 'Query' to align rows
        dfs = [df.sort_values('Query').reset_index(drop=True) for df in dfs]

        # Concatenate DataFrames along a new axis
        stacked = pd.concat(dfs, axis=0, keys=range(len(dfs)))

        numeric_col = 'Total (ms)'

        # Check if all DataFrames have the required column
        if not all(numeric_col in df.columns for df in dfs):
            print(f"Not all files contain the '{numeric_col}' column. Skipping.")
            continue

        # Stack and average
        dfs = [df.sort_values('Query').reset_index(drop=True) for df in dfs]
        stacked = pd.concat(dfs, axis=0, keys=range(len(dfs)))
        averaged = stacked.groupby(level=1)[[numeric_col]].mean().reset_index(drop=True)
        averaged['Query'] = dfs[0]['Query']
        averaged = averaged[['Query', numeric_col]]
        averaged = natural_sort_queries(averaged)

        # Move TOTAL row to the top if it exists
        if 'TOTAL' in averaged['Query'].values:
            total_row = averaged[averaged['Query'] == 'TOTAL']
            other_rows = averaged[averaged['Query'] != 'TOTAL']
            averaged = pd.concat([total_row, other_rows]).reset_index(drop=True)

        # Save to CSV
        output_filename = os.path.join(search_dir, f'avg_results.csv')
        averaged.to_csv(output_filename, index=False)
        print(f"Created average file: {output_filename}")
