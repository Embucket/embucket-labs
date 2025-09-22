#!/usr/bin/env python3
"""
Script to count rows for all dbt models and insert results into dbt_snowplow_results_models table.
"""

import os
import sys
import snowflake.connector
from pathlib import Path
from db_connections import create_embucket_connection, create_snowflake_connection


def get_row_count_queries():
    """Get row count queries for all dbt models (works for both Snowflake and Embucket)."""
    queries = [
        # Seeds
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_ga4_source_categories' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_ga4_source_categories",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_geo_country_mapping' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_geo_country_mapping",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_rfc_5646_language_mapping' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_dim_rfc_5646_language_mapping",
        
        # Incremental models
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_quarantined_sessions' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_quarantined_sessions",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_incremental_manifest' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_incremental_manifest",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_sessions_lifecycle_manifest' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SNOWPLOW_MANIFEST.snowplow_web_base_sessions_lifecycle_manifest",
        
        # Scratch tables
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_new_event_limits' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_new_event_limits",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_sessions_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_sessions_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_events_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_base_events_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_consent_events_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_consent_events_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_engaged_time' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_engaged_time",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_scroll_depth' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_pv_scroll_depth",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_sessions_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_sessions_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vital_events_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vital_events_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_page_views_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_page_views_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vitals_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_vitals_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_sessions_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_sessions_this_run",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_aggs' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_aggs",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_lasts' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_lasts",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_this_run' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_SCRATCH.snowplow_web_users_this_run",
        
        # Derived models
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_user_mapping' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_user_mapping",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_log' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_log",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_sessions' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_sessions",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_page_views' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_page_views",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_cmp_stats' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_cmp_stats",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_versions' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_versions",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vitals' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vitals",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_users' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_users",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vital_measurements' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_vital_measurements",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_scope_status' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_scope_status",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_totals' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_consent_totals",
        "SELECT 'PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_users' AS table_name, COUNT(*) AS row_count FROM PUBLIC_SNOWPLOW_MANIFEST_DERIVED.snowplow_web_users"
    ]
    
    return queries


def execute_row_count_queries(conn, queries):
    """Execute row count queries and return results."""
    cursor = conn.cursor()
    results = []
    
    for i, query in enumerate(queries, 1):
        try:
            print(f"Executing query {i}/{len(queries)}: {query[:60]}...")
            cursor.execute(query)
            result = cursor.fetchone()
            if result:
                table_name = result[0]
                row_count = result[1]
                results.append((table_name, row_count))
                print(f"✓ {table_name}: {row_count} rows")
            else:
                print(f"⚠ No result for query {i}")
        except Exception as e:
            print(f"⚠ Error executing query {i}: {e}")
            # Continue with next query
    
    cursor.close()
    return results


def add_row_count_column_if_not_exists(conn):
    """Add row_count column to dbt_snowplow_results_models table if it doesn't exist."""
    cursor = conn.cursor()
    
    try:
        # Try to add the row_count column
        alter_query = "ALTER TABLE dbt_snowplow_results_models ADD COLUMN row_count INTEGER"
        cursor.execute(alter_query)
        print("✓ Added row_count column to dbt_snowplow_results_models table")
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            print("✓ row_count column already exists")
        else:
            print(f"⚠ Error adding row_count column: {e}")
    
    cursor.close()


def update_row_counts_in_results_table(conn, results, target):
    """Update existing records in dbt_snowplow_results_models table with row counts."""
    cursor = conn.cursor()
    
    try:
        # Update each row count result
        for table_name, row_count in results:
            # Extract just the model name from the full table name
            model_name = table_name.split('.')[-1]  # Get the last part after the last dot
            
            # Use different parameter placeholders for different databases
            if target.lower() == 'snowflake':
                update_query = """
                UPDATE dbt_snowplow_results_models 
                SET row_count = %s, updated_at = CURRENT_TIMESTAMP()
                WHERE model_name = %s AND target_database = %s
                """
            else:  # embucket
                update_query = """
                UPDATE dbt_snowplow_results_models 
                SET row_count = ?, updated_at = CURRENT_TIMESTAMP()
                WHERE model_name = ? AND target_database = ?
                """
            
            cursor.execute(update_query, (
                row_count,
                model_name,
                target
            ))
            
            if cursor.rowcount > 0:
                print(f"✓ Updated {model_name}: {row_count} rows")
            else:
                print(f"⚠ No matching record found for {model_name}")
        
        print(f"✓ Updated row counts for {len(results)} models in dbt_snowplow_results_models")
        
    except Exception as e:
        print(f"⚠ Error updating row counts: {e}")
    
    cursor.close()


def main():
    """Main function to count table rows and update results."""
    # Parse command line arguments
    target = None
    
    if len(sys.argv) > 1:
        target = sys.argv[1].lower()
        if target not in ['snowflake', 'embucket']:
            print("Error: Target must be 'snowflake' or 'embucket'")
            print("Usage: python count_table_rows.py [snowflake|embucket]")
            sys.exit(1)
    else:
        print("Error: Please specify target database")
        print("Usage: python count_table_rows.py [snowflake|embucket]")
        sys.exit(1)
    
    print(f"=== Counting Table Rows for {target.upper()} Database ===")
    
    try:
        print(f"Connecting to {target.upper()}...")
        
        if target == 'snowflake':
            conn = create_snowflake_connection()
        else:
            conn = create_embucket_connection()
        
        print(f"✓ Connected to {target.upper()} successfully")
        
        # Get row count queries
        queries = get_row_count_queries()
        print(f"Found {len(queries)} tables to count")
        
        # Add row_count column if it doesn't exist
        print("Adding row_count column if needed...")
        add_row_count_column_if_not_exists(conn)
        
        # Execute row count queries
        print("Executing row count queries...")
        results = execute_row_count_queries(conn, queries)
        
        # Update existing records in dbt_snowplow_results_models table with row counts
        print("Updating row counts in dbt_snowplow_results_models...")
        update_row_counts_in_results_table(conn, results, target)
        
        conn.close()
        print("✓ Row count process completed successfully!")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()