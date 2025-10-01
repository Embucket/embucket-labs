"""
TPC-DS benchmark utilities package.

This package contains all TPC-DS related functionality including:
- Table name configuration and parametrization
- Query definitions with parametrized table names
- DDL statements with parametrized table names

Main exports:
- parametrize_tpcds_queries: Parametrize TPC-DS queries (requires explicit parameter)
- parametrize_tpcds_ddl: Parametrize TPC-DS DDL statements (requires explicit parameter)
- TPCDS_TABLE_NAMES: Raw table name mappings
- get_table_names: Get parametrized table names (requires explicit parameter)
- parametrize_tpcds_statements: Generic parametrization function (requires explicit parameter)

Note: All functions require explicit fully_qualified_names_for_embucket parameter.
No pre-computed constants are provided to enforce explicit parameter usage.
"""

from .tpcds_table_names import (
    TPCDS_TABLE_NAMES,
    get_table_names,
    parametrize_tpcds_statements
)

from .tpcds_queries import (
    parametrize_tpcds_queries,
)

from .tpcds_ddl import (
    parametrize_tpcds_ddl,
)

__all__ = [
    # Table names and core functions
    'TPCDS_TABLE_NAMES',
    'get_table_names',
    'parametrize_tpcds_statements',

    # Query functions
    'parametrize_tpcds_queries',

    # DDL functions
    'parametrize_tpcds_ddl',
]
