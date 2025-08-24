#!/usr/bin/env python3
"""
Script to load Snowplow events data into Embucket database using Snowflake connector.
"""

import os
import sys
import snowflake.connector
from pathlib import Path

def get_connection_config():
    """Get connection configuration for Embucket."""
    return {
        'host': os.getenv('EMBUCKET_HOST', 'localhost'),
        'port': int(os.getenv('EMBUCKET_PORT', 3000)),
        'protocol': os.getenv('EMBUCKET_PROTOCOL', 'http'),
        'user': os.getenv('EMBUCKET_USER', 'embucket'),
        'password': os.getenv('EMBUCKET_PASSWORD', 'embucket'),
        'account': os.getenv('EMBUCKET_ACCOUNT', 'acc'),
        'warehouse': os.getenv('EMBUCKET_WAREHOUSE', ''),
        'database': os.getenv('EMBUCKET_DATABASE', 'embucket'),
        'schema': os.getenv('EMBUCKET_SCHEMA', 'public_snowplow_manifest'),
    }

def copy_file_to_data_dir(source_file, data_dir="./datasets"):
    """Copy the events.csv file to the data directory."""
    import shutil
    import subprocess
    
    # Create data directory if it doesn't exist
    os.makedirs(data_dir, exist_ok=True)
    
    # Copy file to app data directory
    target_file = os.path.join(data_dir, "events.csv")
    try:
        shutil.copy2(source_file, target_file)
        print(f"✓ Copied {source_file} to {target_file}")
    except PermissionError:
        # Use sudo if permission denied
        import subprocess
        subprocess.run(['sudo', 'cp', source_file, target_file], check=True)
        subprocess.run(['sudo', 'chmod', '644', target_file], check=True)
        print(f"✓ Copied {source_file} to {target_file} (with sudo)")

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
    print("=== Loading Snowplow Events Data into Embucket Database ===")
    
    # Configuration
    script_dir = Path(__file__).parent
    
    # Check if a file argument was provided
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        events_file = Path(input_file)
        
        # Check if the input file exists
        if not events_file.exists():
            print(f"Error: {events_file} not found")
            sys.exit(1)
    else:
        # Default behavior - use events.csv in script directory
        events_file = script_dir / "events.csv"
    
    sql_script = script_dir / "load_events_data.sql"
    
    # Check if required files exist
    if not events_file.exists():
        print(f"Error: {events_file} not found")
        sys.exit(1)
    
    if not sql_script.exists():
        print(f"Error: {sql_script} not found")
        sys.exit(1)
    
    # Copy file to data directory
    print(f"Copying {events_file} to data directory...")
    copy_file_to_data_dir(str(events_file))
    
    # Connect to Embucket
    print("Connecting to Embucket...")
    config = get_connection_config()
    
    try:
        conn = snowflake.connector.connect(**config)
        print("✓ Connected to Embucket successfully")
        
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
    
    print("\n=== Data Load Process Complete ===")
    print("\nTo verify the data was loaded correctly, you can run:")
    print("python3 -c \"import snowflake.connector; conn = snowflake.connector.connect(host='localhost', port=3000, protocol='http', user='embucket', password='embucket', account='acc', database='embucket', schema='public_snowplow_manifest'); cursor = conn.cursor(); cursor.execute('SELECT COUNT(*) as total_rows FROM events'); print(cursor.fetchone()[0]); conn.close()\"")
    
    print("\nTo view sample data:")
    print("python3 -c \"import snowflake.connector; conn = snowflake.connector.connect(host='localhost', port=3000, protocol='http', user='embucket', password='embucket', account='acc', database='embucket', schema='public_snowplow_manifest'); cursor = conn.cursor(); cursor.execute('SELECT event_id, event, user_id, collector_tstamp, page_url FROM events LIMIT 5'); [print(row) for row in cursor.fetchall()]; conn.close()\"")

if __name__ == "__main__":
    main() 