"""
TPC-H table names configuration.

This module defines the 8 standard TPC-H source table names and their
corresponding placeholder names used for parametrization.
"""

# The 8 TPC-H source table names with their parametrization placeholders
TPCH_TABLE_NAMES = {
    'LINEITEM_TABLE': 'lineitem',
    'CUSTOMER_TABLE': 'customer',
    'ORDERS_TABLE': 'orders',
    'PART_TABLE': 'part',
    'SUPPLIER_TABLE': 'supplier',
    'PARTSUPP_TABLE': 'partsupp',
    'NATION_TABLE': 'nation',
    'REGION_TABLE': 'region'
}

def get_table_names(fully_qualified_names_for_embucket, use_custom_dataset=False):
    """
    Get table names dictionary with optional fully qualified naming.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use Snowflake built-in tables or custom tables based on use_custom_dataset.
        use_custom_dataset (bool): Only applies when fully_qualified_names_for_embucket=False (Snowflake).
                                   If False (default), use Snowflake's built-in TPC-H tables (SNOWFLAKE_SAMPLE_DATA.TPCH_SFx).
                                   If True, use custom tables in user's schema.

    Returns:
        dict: Dictionary mapping placeholder names to actual table names.
    """
    import os

    table_names = TPCH_TABLE_NAMES.copy()

    if fully_qualified_names_for_embucket:
        # Embucket: Get database and schema from environment variables
        database = os.environ['EMBUCKET_DATABASE']
        schema = os.environ['EMBUCKET_SCHEMA']

        # Create fully qualified table names
        for key, table_name in table_names.items():
            table_names[key] = f"{database}.{schema}.{table_name}"
    elif not use_custom_dataset:
        # Snowflake with built-in sample data (default)
        # Determine scale factor from DATASET_PATH (e.g., "tpch/01" -> SF1, "tpch/10" -> SF10)
        dataset_path = os.environ.get('DATASET_PATH', 'tpch/01')

        # Extract scale factor from path
        # Expected format: "tpch/01", "tpch/10", "tpch/100", "tpch/1000"
        parts = dataset_path.split('/')
        if len(parts) >= 2:
            scale_str = parts[1].lstrip('0') or '1'  # Remove leading zeros, default to '1'
            scale_factor = int(scale_str)
        else:
            scale_factor = 1

        # Snowflake's SNOWFLAKE_SAMPLE_DATA has: TPCH_SF1, TPCH_SF10, TPCH_SF100, TPCH_SF1000
        # Map our scale factor to the available schemas
        # Note: Not all accounts may have all scale factors available
        if scale_factor >= 1000:
            schema_name = "TPCH_SF1000"
        elif scale_factor >= 100:
            schema_name = "TPCH_SF100"
        elif scale_factor >= 10:
            schema_name = "TPCH_SF10"
        else:
            schema_name = "TPCH_SF1"

        # Create fully qualified table names using Snowflake sample data
        for key, table_name in table_names.items():
            table_names[key] = f"SNOWFLAKE_SAMPLE_DATA.{schema_name}.{table_name}"
    # else: use_custom_dataset=True, keep bare table names for custom tables in user's schema

    return table_names


def parametrize_tpch_statements(statements_raw, fully_qualified_names_for_embucket, use_custom_dataset=False):
    """
    Generic function to parametrize TPC-H statements (queries or DDL) with table names.

    Args:
        statements_raw (list): List of (name, statement_sql) tuples with placeholder table names.
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use Snowflake built-in or custom tables based on use_custom_dataset.
        use_custom_dataset (bool): Only applies when fully_qualified_names_for_embucket=False (Snowflake).
                                   If False (default), use Snowflake's built-in TPC-H tables.
                                   If True, use custom tables in user's schema.

    Returns:
        list: A list of (name, parametrized_statement) tuples.
    """
    # Get table names with appropriate formatting
    table_names = get_table_names(fully_qualified_names_for_embucket, use_custom_dataset)

    parametrized_statements = []

    for name, statement_sql in statements_raw:
        # Replace table name placeholders
        parametrized_sql = statement_sql.format(**table_names)
        parametrized_statements.append((name, parametrized_sql))

    return parametrized_statements
