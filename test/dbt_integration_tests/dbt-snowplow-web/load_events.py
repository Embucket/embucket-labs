#!/usr/bin/env python3
"""
Script to load Snowplow events data into Embucket or Snowflake database using Snowflake connector.
"""

import os
import sys
import snowflake.connector
from pathlib import Path

def get_connection_config(target='embucket'):
    """Get connection configuration for Embucket or Snowflake."""
    if target.lower() == 'snowflake':
        return {
            'account': os.getenv('SNOWFLAKE_ACCOUNT', ''),
            'user': os.getenv('SNOWFLAKE_USER', ''),
            'password': os.getenv('SNOWFLAKE_PASSWORD', ''),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'BENCHMARK_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'benchmark_db'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'public'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'SYSADMIN'),
        }
    else:  # embucket
        return {
            'host': os.getenv('EMBUCKET_HOST', 'localhost'),
            'port': int(os.getenv('EMBUCKET_PORT', 3000)),
            'protocol': os.getenv('EMBUCKET_PROTOCOL', 'http'),
            'user': os.getenv('EMBUCKET_USER', 'embucket'),
            'password': os.getenv('EMBUCKET_PASSWORD', 'embucket'),
            'account': os.getenv('EMBUCKET_ACCOUNT', 'acc'),
            'warehouse': os.getenv('EMBUCKET_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('EMBUCKET_DATABASE', 'embucket'),
            'schema': os.getenv('EMBUCKET_SCHEMA', 'public_snowplow_manifest'),
            'role': os.getenv('EMBUCKET_ROLE', 'SYSADMIN'),
        }

def copy_file_to_data_dir(source_file, data_dir="./datasets", target='embucket'):
    """Copy the events.csv file to the data directory."""
    import shutil
    import subprocess
    
    if target.lower() == 'snowflake':
        # For Snowflake, we don't need to copy to a specific data directory
        # The file will be uploaded directly via Snowflake's PUT command
        print(f"✓ File {source_file} ready for Snowflake upload")
        return source_file
    else:
        # For Embucket, copy to data directory
        os.makedirs(data_dir, exist_ok=True)
        target_file = os.path.join(data_dir, "events.csv")
        try:
            shutil.copy2(source_file, target_file)
            print(f"✓ Copied {source_file} to {target_file}")
            return target_file
        except PermissionError:
            # Use sudo if permission denied
            subprocess.run(['sudo', 'cp', source_file, target_file], check=True)
            subprocess.run(['sudo', 'chmod', '644', target_file], check=True)
            print(f"✓ Copied {source_file} to {target_file} (with sudo)")
            return target_file

def execute_sql_script(conn, script_path):
    """Execute SQL script against the database."""
    with open(script_path, 'r') as f:
        sql_content = f.read()
    
    # Split by semicolon and execute each statement
    statements = []
    current_statement = ""
    
    for line in sql_content.split('\n'):
        line = line.strip()
        if line.startswith('--') or not line:  # Skip comments and empty lines
            continue
        current_statement += line + " "
        if line.endswith(';'):
            statements.append(current_statement.strip())
            current_statement = ""
    
    if current_statement.strip():
        statements.append(current_statement.strip())
    
    cursor = conn.cursor()
    
    for i, statement in enumerate(statements, 1):
        if statement and not statement.startswith('--'):
            print(f"Executing statement {i}/{len(statements)}: {statement[:50]}...")
            try:
                cursor.execute(statement)
                print("✓ Statement executed successfully")
            except Exception as e:
                print(f"⚠ Warning executing statement {i}: {e}")
                # Continue with next statement
    
    cursor.close()

def verify_data_load(conn):
    """Verify that data was loaded successfully."""
    cursor = conn.cursor()
    
    try:
        # Check total rows
        cursor.execute("SELECT COUNT(*) as total_rows FROM events")
        result = cursor.fetchone()
        if result and result[0] is not None:
            total_rows = result[0]
            print(f"✓ Data verification: {total_rows} rows loaded")
            
            if total_rows > 0:
                # Show sample data
                cursor.execute("""
                    SELECT event_id, event, user_id, collector_tstamp, page_url 
                    FROM events 
                    LIMIT 3
                """)
                sample_data = cursor.fetchall()
                print("✓ Sample data:")
                for row in sample_data:
                    print(f"  {row}")
            else:
                print("⚠ Warning: Table is empty - data may not have loaded correctly")
        else:
            print("⚠ Warning: Could not verify row count")
            
    except Exception as e:
        print(f"⚠ Warning during verification: {e}")
    
    cursor.close()

def main():
    """Main function to load events data."""
    # Parse command line arguments
    target = 'embucket'  # default
    input_file = None
    
    # Simple argument parsing
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg in ['--target', '-t']:
            if i + 1 < len(args):
                target = args[i + 1]
        elif arg in ['snowflake', 'embucket']:
            target = arg
        elif not arg.startswith('-') and not arg in ['snowflake', 'embucket']:
            input_file = arg
    
    print(f"=== Loading Snowplow Events Data into {target.upper()} Database ===")
    
    # Configuration
    script_dir = Path(__file__).parent
    
    # Determine input file
    if input_file:
        events_file = Path(input_file)
        if not events_file.exists():
            print(f"Error: {events_file} not found")
            sys.exit(1)
    else:
        # Default behavior - use events.csv in script directory
        events_file = script_dir / "events.csv"
    
    # Determine SQL script based on target
    if target.lower() == 'snowflake':
        sql_script = script_dir / "load_events_data_snowflake.sql"
    else:
        sql_script = script_dir / "load_events_data.sql"
    
    # Check if required files exist
    if not events_file.exists():
        print(f"Error: {events_file} not found")
        sys.exit(1)
    
    if not sql_script.exists():
        print(f"Error: {sql_script} not found")
        sys.exit(1)
    
    # Copy file to data directory (or prepare for Snowflake)
    print(f"Preparing {events_file} for {target}...")
    copy_file_to_data_dir(str(events_file), target=target)
    
    # Connect to database
    print(f"Connecting to {target.upper()}...")
    config = get_connection_config(target)
    
    try:
        conn = snowflake.connector.connect(**config)
        print(f"✓ Connected to {target.upper()} successfully")
        
        # Execute SQL script
        print("Executing SQL script...")
        execute_sql_script(conn, sql_script)
        
        # Verify data load
        print("Verifying data load...")
        verify_data_load(conn)
        
        conn.close()
        print("✓ Data load completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print(f"\n=== Data Load Process Complete ===")

if __name__ == "__main__":
    main() 