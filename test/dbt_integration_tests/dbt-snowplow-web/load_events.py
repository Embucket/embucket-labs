#!/usr/bin/env python3
"""
Script to load Snowplow events data into Embucket or Snowflake database using Snowflake connector.
"""

import os
import sys
import snowflake.connector
from pathlib import Path
from db_connections import create_embucket_connection, create_snowflake_connection, get_connection_config, copy_file_to_data_dir


def execute_sql_script(conn, script_path, filename=None):
    """Execute SQL script against the database."""
    with open(script_path, 'r') as f:
        sql_content = f.read()
    
    # Replace filename placeholders if filename is provided
    if filename:
        sql_content = sql_content.replace('events_yesterday.csv', filename)
        sql_content = sql_content.replace('events_today.csv', filename)
    
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
    is_incremental = False
    input_file = None
    
    # Simple argument parsing
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg in ['--target', '-t']:
            if i + 1 < len(args):
                target = args[i + 1]
        elif arg in ['snowflake', 'embucket']:
            target = arg
        elif arg in ['true', 'false']:
            is_incremental = (arg == 'true')
        elif not arg.startswith('-') and not arg in ['snowflake', 'embucket', 'true', 'false']:
            if input_file is None:
                input_file = arg
            elif target == 'embucket':  # If target is still default, treat second arg as target
                target = arg
    
    # Determine input file based on incremental flag
    if not input_file:
        if is_incremental:
            input_file = 'events_today.csv'
            print("Incremental run - using events_today.csv")
        else:
            input_file = 'events_yesterday.csv'
            print("First run - using events_yesterday.csv")
    
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
    
    try:
        if target.lower() == 'snowflake':
            conn = create_snowflake_connection()
        else:
            conn = create_embucket_connection()
        
        print(f"✓ Connected to {target.upper()} successfully")
        
        # Execute SQL script
        print("Executing SQL script...")
        execute_sql_script(conn, sql_script, events_file.name)
        
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