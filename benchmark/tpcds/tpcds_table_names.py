"""
TPC-DS table names configuration.

This module defines the 24 standard TPC-DS source table names and their
corresponding placeholder names used for parametrization.
"""

# The TPC-DS source table names with their parametrization placeholders
TPCDS_TABLE_NAMES = {
    'CALL_CENTER_TABLE': 'call_center',
    'CATALOG_PAGE_TABLE': 'catalog_page',
    'CATALOG_RETURNS_TABLE': 'catalog_returns',
    'CATALOG_SALES_TABLE': 'catalog_sales',
    'CUSTOMER_ADDRESS_TABLE': 'customer_address',
    'CUSTOMER_DEMOGRAPHICS_TABLE': 'customer_demographics',
    'CUSTOMER_TABLE': 'customer',
    'DATE_DIM_TABLE': 'date_dim',
    'HOUSEHOLD_DEMOGRAPHICS_TABLE': 'household_demographics',
    'INCOME_BAND_TABLE': 'income_band',
    'INVENTORY_TABLE': 'inventory',
    'ITEM_TABLE': 'item',
    'PROMOTION_TABLE': 'promotion',
    'REASON_TABLE': 'reason',
    'SHIP_MODE_TABLE': 'ship_mode',
    'STORE_RETURNS_TABLE': 'store_returns',
    'STORE_SALES_TABLE': 'store_sales',
    'STORE_TABLE': 'store',
    'TIME_DIM_TABLE': 'time_dim',
    'WAREHOUSE_TABLE': 'warehouse',
    'WEB_PAGE_TABLE': 'web_page',
    'WEB_RETURNS_TABLE': 'web_returns',
    'WEB_SALES_TABLE': 'web_sales',
    'WEB_SITE_TABLE': 'web_site'
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

    table_names = TPCDS_TABLE_NAMES.copy()

    if fully_qualified_names_for_embucket:
        # Get database and schema from environment variables
        database = os.environ['EMBUCKET_DATABASE']
        schema = os.environ['EMBUCKET_SCHEMA']

        # Create fully qualified table names
        for key, table_name in table_names.items():
            table_names[key] = f"{database}.{schema}.{table_name}"

    return table_names


def parametrize_tpcds_statements(statements_raw, fully_qualified_names_for_embucket):
    """
    Generic function to parametrize TPC-DS statements (queries or DDL) with table names.

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
