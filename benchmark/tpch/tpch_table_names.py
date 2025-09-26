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

def get_table_names(fully_qualified_names_for_embucket):
    """
    Get table names dictionary with optional fully qualified naming.

    Args:
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        dict: Dictionary mapping placeholder names to actual table names.
    """
    import os

    table_names = TPCH_TABLE_NAMES.copy()

    if fully_qualified_names_for_embucket:
        # Get database and schema from environment variables
        database = os.environ['EMBUCKET_DATABASE']
        schema = os.environ['EMBUCKET_SCHEMA']

        # Create fully qualified table names
        for key, table_name in table_names.items():
            table_names[key] = f"{database}.{schema}.{table_name}"

    return table_names


def parametrize_tpch_statements(statements_raw, fully_qualified_names_for_embucket):
    """
    Generic function to parametrize TPC-H statements (queries or DDL) with table names.

    Args:
        statements_raw (list): List of (name, statement_sql) tuples with placeholder table names.
        fully_qualified_names_for_embucket (bool): Required. If True, use EMBUCKET_DATABASE.EMBUCKET_SCHEMA.tablename format.
                                                   If False, use just the default table names.

    Returns:
        list: A list of (name, parametrized_statement) tuples.
    """
    # Get table names with appropriate formatting
    table_names = get_table_names(fully_qualified_names_for_embucket)

    parametrized_statements = []

    for name, statement_sql in statements_raw:
        # Replace table name placeholders
        parametrized_sql = statement_sql.format(**table_names)
        parametrized_statements.append((name, parametrized_sql))

    return parametrized_statements
