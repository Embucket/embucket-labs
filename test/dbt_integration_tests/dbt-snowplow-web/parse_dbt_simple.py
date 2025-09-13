#!/usr/bin/env python3
"""
Simple script to parse dbt results and load them into Snowflake database.
"""

import os
import sys
import re
import snowflake.connector
from datetime import datetime

def get_connection_config():
    """Get Snowflake connection configuration."""
    return {
        'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
        'user': os.getenv('SNOWFLAKE_USER', ''),
        'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
        'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'BENCHMARK_WH'),
        'database': os.getenv('SNOWFLAKE_DATABASE', 'benchmark_db'),
        'schema': os.getenv('SNOWFLAKE_SCHEMA', 'public'),
        'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
    }

def parse_duration(duration_str):
    """Parse duration string and convert to seconds"""
    if not duration_str:
        return 0.0
    
    # Remove any non-numeric characters except decimal point and 'm' or 's'
    duration_str = duration_str.strip()
    
    # Handle minutes format (e.g., "1m 30s", "2m", "1.5m")
    if 'm' in duration_str:
        # Split by 'm' and 's' to get minutes and seconds parts
        parts = re.split(r'[ms]', duration_str)
        minutes = 0.0
        seconds = 0.0
        
        if len(parts) >= 2 and parts[0]:
            minutes = float(parts[0])
        if len(parts) >= 3 and parts[1]:
            seconds = float(parts[1])
        
        return minutes * 60 + seconds
    
    # Handle seconds format (e.g., "2.5s", "30s")
    elif 's' in duration_str:
        seconds_str = duration_str.replace('s', '')
        return float(seconds_str) if seconds_str else 0.0
    
    # If no unit specified, assume seconds
    else:
        return float(duration_str) if duration_str else 0.0

def parse_dbt_output(dbt_output):
    """Parse dbt output and extract model information."""
    results = []
    
    # Remove ANSI color codes
    ansi_escape = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')
    clean_output = ansi_escape.sub('', dbt_output)
    
    # Join wrapped lines
    lines = clean_output.split('\n')
    joined_lines = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # If line ends with space and next line starts with [, join them
        if i + 1 < len(lines) and line.endswith(' ') and lines[i + 1].strip().startswith('['):
            line = line + lines[i + 1].strip()
            i += 1
        joined_lines.append(line)
        i += 1
    
    current_timestamp = datetime.now()
    
    # Extract metadata from dbt output
    dbt_version = "unknown"
    adapter_type = "unknown"
    total_models = 0
    pass_count = 0
    warn_count = 0
    error_count = 0
    skip_count = 0
    
    for line in joined_lines:
        # Extract dbt version
        if "Running with dbt=" in line:
            version_match = re.search(r'dbt=([\d.]+)', line)
            if version_match:
                dbt_version = version_match.group(1)
        
        # Extract adapter type
        if "adapter type:" in line:
            adapter_match = re.search(r'adapter type: (\w+)', line)
            if adapter_match:
                adapter_type = adapter_match.group(1)
        
        # Extract total counts
        if "Done. PASS=" in line:
            counts_match = re.search(r'PASS=(\d+) WARN=(\d+) ERROR=(\d+) SKIP=(\d+) TOTAL=(\d+)', line)
            if counts_match:
                pass_count = int(counts_match.group(1))
                warn_count = int(counts_match.group(2))
                error_count = int(counts_match.group(3))
                skip_count = int(counts_match.group(4))
                total_models = int(counts_match.group(5))
    
    # Simple patterns that should work
    for line in joined_lines:
        line = line.strip()
        
        # Look for lines with model results
        if 'OK' in line and ('model' in line or 'seed' in line) and '[' in line and ']' in line:
            # Extract order number
            order_match = re.search(r'(\d+ of \d+)', line)
            order = order_match.group(1) if order_match else "unknown"
            
            # Extract model name using regex to find the pattern: model_type model_name
            model_name_match = re.search(r'(sql \w+ model|seed file) ([^\s]+)', line)
            if model_name_match:
                model_name = model_name_match.group(2)  # The model name part
            else:
                model_name = "unknown"
            
            # Extract duration - handle both seconds and minutes
            duration_match = re.search(r'(\d+\.?\d*[ms]?)\s*\]', line)
            duration_str = duration_match.group(1) if duration_match else "0s"
            duration = parse_duration(duration_str)
            
            # Extract rows affected
            rows_match = re.search(r'\[(SUCCESS|CREATE) (\d+)', line)
            rows = int(rows_match.group(2)) if rows_match else 0
            
            # Determine result type
            if 'SUCCESS' in line:
                result = 'SUCCESS'
            elif 'CREATE' in line:
                result = 'CREATE'
            else:
                result = 'OK'
            
            # Determine model type
            if 'seed file' in line:
                model_type = 'seed'
            elif 'incremental model' in line:
                model_type = 'incremental'
            elif 'table model' in line:
                model_type = 'table'
            else:
                model_type = 'model'
            
            results.append({
                'timestamp': current_timestamp,
                'model_name': model_name,
                'model_type': model_type,
                'result': result,
                'duration': duration,
                'rows_affected': rows,
                'order': order,
                'target': 'snowflake',
                'dbt_version': dbt_version,
                'adapter_type': adapter_type,
                'total_models': total_models,
                'pass_count': pass_count,
                'warn_count': warn_count,
                'error_count': error_count,
                'skip_count': skip_count
            })
        
        # Look for ERROR lines
        elif 'ERROR' in line and 'model' in line and '[' in line and ']' in line:
            order_match = re.search(r'(\d+ of \d+)', line)
            order = order_match.group(1) if order_match else "unknown"
            
            # Extract model name using regex to find the pattern: model_type model_name
            model_name_match = re.search(r'(sql \w+ model|seed file) ([^\s]+)', line)
            if model_name_match:
                model_name = model_name_match.group(2)  # The model name part
            else:
                model_name = "unknown"
            
            # Extract duration - handle both seconds and minutes
            duration_match = re.search(r'(\d+\.?\d*[ms]?)\s*\]', line)
            duration_str = duration_match.group(1) if duration_match else "0s"
            duration = parse_duration(duration_str)
            
            results.append({
                'timestamp': current_timestamp,
                'model_name': model_name,
                'model_type': 'table',
                'result': 'ERROR',
                'duration': duration,
                'rows_affected': 0,
                'order': order,
                'target': 'snowflake',
                'dbt_version': dbt_version,
                'adapter_type': adapter_type,
                'total_models': total_models,
                'pass_count': pass_count,
                'warn_count': warn_count,
                'error_count': error_count,
                'skip_count': skip_count
            })
        
        # Look for SKIP lines
        elif 'SKIP' in line and 'relation' in line:
            order_match = re.search(r'(\d+ of \d+)', line)
            order = order_match.group(1) if order_match else "unknown"
            
            # Extract model name from SKIP relation line
            relation_match = re.search(r'relation ([^\s]+)', line)
            model_name = relation_match.group(1) if relation_match else "unknown"
            
            results.append({
                'timestamp': current_timestamp,
                'model_name': model_name,
                'model_type': 'skipped',
                'result': 'SKIP',
                'duration': 0.0,
                'rows_affected': 0,
                'order': order,
                'target': 'snowflake',
                'dbt_version': dbt_version,
                'adapter_type': adapter_type,
                'total_models': total_models,
                'pass_count': pass_count,
                'warn_count': warn_count,
                'error_count': error_count,
                'skip_count': skip_count
            })
    
    return results

def create_results_table(conn, cursor):
    """Create the dbt_snowplow_results_models table if it doesn't exist."""
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS dbt_snowplow_results_models (
        id INTEGER AUTOINCREMENT PRIMARY KEY,
        timestamp TIMESTAMP_NTZ,
        model_name STRING,
        model_type STRING,
        result STRING,
        duration_seconds FLOAT,
        rows_affected INTEGER,
        order_sequence STRING,
        target STRING,
        run_id STRING,
        dbt_version STRING,
        adapter_type STRING,
        total_models INTEGER,
        pass_count INTEGER,
        warn_count INTEGER,
        error_count INTEGER,
        skip_count INTEGER,
        downloaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """
    
    cursor.execute(create_table_sql)
    conn.commit()
    print("✓ dbt_snowplow_results_models table created/verified")

def load_results_to_snowflake(results, target='snowflake'):
    """Load parsed results into Snowflake."""
    config = get_connection_config()
    
    print(f"=== Loading dbt Results into SNOWFLAKE Database ===")
    print(f"Connecting to SNOWFLAKE...")
    
    try:
        conn = snowflake.connector.connect(**config)
        cursor = conn.cursor()
        print("✓ Connected to SNOWFLAKE successfully")
        
        # Create table
        create_results_table(conn, cursor)
        
        # Generate run_id for this batch
        run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Set the same downloaded_at timestamp for all models in this run
        downloaded_at = datetime.now()
        
        # Insert results
        insert_sql = """
        INSERT INTO dbt_snowplow_results_models 
        (timestamp, model_name, model_type, result, duration_seconds, rows_affected, order_sequence, target, run_id, dbt_version, adapter_type, total_models, pass_count, warn_count, error_count, skip_count, downloaded_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for result in results:
            result['target'] = target
            cursor.execute(insert_sql, (
                result['timestamp'],
                result['model_name'],
                result['model_type'],
                result['result'],
                result['duration'],
                result['rows_affected'],
                result['order'],
                result['target'],
                run_id,
                result['dbt_version'],
                result['adapter_type'],
                result['total_models'],
                result['pass_count'],
                result['warn_count'],
                result['error_count'],
                result['skip_count'],
                downloaded_at
            ))
        
        conn.commit()
        print(f"✓ Loaded {len(results)} dbt results into Snowflake")
        
        # Verify data
        cursor.execute("SELECT COUNT(*) FROM dbt_snowplow_results_models WHERE run_id = %s", (run_id,))
        count = cursor.fetchone()[0]
        print(f"✓ Verification: {count} records loaded for run_id: {run_id}")
        
        # Show summary
        cursor.execute("""
            SELECT 
                result,
                COUNT(*) as count,
                AVG(duration_seconds) as avg_duration,
                SUM(rows_affected) as total_rows
            FROM dbt_snowplow_results_models 
            WHERE run_id = %s
            GROUP BY result
            ORDER BY result
        """, (run_id,))
        
        print("\n=== Results Summary ===")
        for row in cursor.fetchall():
            result, count, avg_duration, total_rows = row
            print(f"{result}: {count} models, avg duration: {avg_duration:.2f}s, total rows: {total_rows}")
        
        cursor.close()
        conn.close()
        print("\n=== Data Load Process Complete ===")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 parse_dbt_simple.py <dbt_output_file> [target]")
        print("Example: python3 parse_dbt_simple.py dbt_output.log snowflake")
        sys.exit(1)
    
    dbt_output_file = sys.argv[1]
    target = sys.argv[2] if len(sys.argv) > 2 else 'snowflake'
    
    # Read dbt output from file
    try:
        with open(dbt_output_file, 'r') as f:
            dbt_output = f.read()
    except FileNotFoundError:
        print(f"Error: File {dbt_output_file} not found")
        sys.exit(1)
    
    # Parse the output
    print("Parsing dbt output...")
    results = parse_dbt_output(dbt_output)
    print(f"✓ Parsed {len(results)} model results")
    
    # Load to Snowflake
    load_results_to_snowflake(results, target)

if __name__ == "__main__":
    main()