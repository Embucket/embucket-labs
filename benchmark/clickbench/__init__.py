"""
ClickBench benchmark utilities package.

This package contains all ClickBench related functionality including:
- Table name configuration and parametrization
- Query definitions with parametrized table names
- DDL statements with parametrized table names

Main exports:
- parametrize_clickbench_queries: Parametrize ClickBench queries (requires explicit parameter)
- parametrize_clickbench_ddl: Parametrize ClickBench DDL statements (requires explicit parameter)
- CLICKBENCH_TABLE_NAMES: Raw table name mappings
- get_table_names: Get parametrized table names (requires explicit parameter)
- parametrize_clickbench_statements: Generic parametrization function (requires explicit parameter)

Note: All functions require explicit fully_qualified_names_for_embucket parameter.
No pre-computed constants are provided to enforce explicit parameter usage.
"""

from .clickbench_table_names import (
    CLICKBENCH_TABLE_NAMES,
    get_table_names,
    parametrize_clickbench_statements
)

from .clickbench_queries import (
    parametrize_clickbench_queries,
)

from .clickbench_ddl import (
    parametrize_clickbench_ddl,
)

__all__ = [
    # Table names and core functions
    'CLICKBENCH_TABLE_NAMES',
    'get_table_names',
    'parametrize_clickbench_statements',

    # Query functions
    'parametrize_clickbench_queries',

    # DDL functions
    'parametrize_clickbench_ddl',
]