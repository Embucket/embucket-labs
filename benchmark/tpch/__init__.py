"""
TPC-H benchmark utilities package.

This package contains all TPC-H related functionality including:
- Table name configuration and parametrization
- Query definitions with parametrized table names
- DDL statements with parametrized table names

Main exports:
- parametrize_tpch_queries: Parametrize TPC-H queries (requires explicit parameter)
- parametrize_tpch_ddl: Parametrize TPC-H DDL statements (requires explicit parameter)
- TPCH_TABLE_NAMES: Raw table name mappings
- get_table_names: Get parametrized table names (requires explicit parameter)
- parametrize_tpch_statements: Generic parametrization function (requires explicit parameter)

Note: All functions require explicit fully_qualified_names_for_embucket parameter.
No pre-computed constants are provided to enforce explicit parameter usage.
"""

from .tpch_table_names import (
    TPCH_TABLE_NAMES,
    get_table_names,
    parametrize_tpch_statements
)

from .tpch_queries import (
    parametrize_tpch_queries,
)

from .tpch_ddl import (
    parametrize_tpch_ddl,
)

__all__ = [
    # Table names and core functions
    'TPCH_TABLE_NAMES',
    'get_table_names',
    'parametrize_tpch_statements',

    # Query functions
    'parametrize_tpch_queries',

    # DDL functions
    'parametrize_tpch_ddl',
]
