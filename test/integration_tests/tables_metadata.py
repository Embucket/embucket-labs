from collections import OrderedDict
from decimal import Decimal
from pyspark.sql.types import (
    IntegerType, LongType, StringType,
    DecimalType, DateType, TimestampType, DoubleType, BooleanType,
)


# Map simplified SQL types to Python type checks
TYPE_CHECKS = {
    "BIGINT": lambda v: isinstance(v, int),
    "INT": lambda v: isinstance(v, int),
    "DECIMAL": lambda v: isinstance(v, (Decimal, float, int)),  # Arrow may materialize decimals as Decimal or float
    "VARCHAR": lambda v: isinstance(v, str),
    "DATE": lambda v: hasattr(v, "year") and hasattr(v, "month") and hasattr(v, "day"),  # datetime.date
    "TIMESTAMP": lambda v: hasattr(v, "year") and hasattr(v, "hour"),  # datetime-like
    "DOUBLE": lambda v: isinstance(v, float),
    "BOOLEAN": lambda v: isinstance(v, bool) or v is None,
}


TABLE_METADATA = {
    "nation": OrderedDict([
        ("n_nationkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("n_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("n_regionkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("n_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),  # nullable for coverage
    ]),
    "region": OrderedDict([
        ("r_regionkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("r_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("r_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "supplier": OrderedDict([
        ("s_suppkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("s_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("s_address", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("s_nationkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("s_phone", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("s_acctbal", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("s_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "part": OrderedDict([
        ("p_partkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("p_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_mfgr", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_brand", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_type", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_size", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("p_container", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("p_retailprice", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("p_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "partsupp": OrderedDict([
        ("ps_partkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("ps_suppkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("ps_availqty", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("ps_supplycost", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("ps_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "customer": OrderedDict([
        ("c_custkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("c_name", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_address", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_nationkey", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("c_phone", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_acctbal", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("c_mktsegment", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("c_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "orders": OrderedDict([
        ("o_orderkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("o_custkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("o_orderstatus", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("o_totalprice", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("o_orderdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("o_orderpriority", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("o_clerk", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("o_shippriority", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("o_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "lineitem": OrderedDict([
        ("l_orderkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_partkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_suppkey", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_linenumber", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("l_quantity", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_extendedprice", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_discount", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_tax", {"type": "DECIMAL(15,2)", "nullable": False, "spark": lambda: DecimalType(15, 2)}),
        ("l_returnflag", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_linestatus", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_shipdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("l_commitdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("l_receiptdate", {"type": "DATE", "nullable": False, "spark": DateType}),
        ("l_shipinstruct", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_shipmode", {"type": "VARCHAR", "nullable": False, "spark": StringType}),
        ("l_comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "datatypes_test": OrderedDict([
        ("id", {"type": "INT", "nullable": False, "spark": IntegerType}),
        ("ts_col", {"type": "TIMESTAMP", "nullable": True, "spark": TimestampType}),
        ("double_col", {"type": "DOUBLE", "nullable": True, "spark": DoubleType}),
        ("bool_col", {"type": "BOOLEAN", "nullable": True, "spark": BooleanType}),
        ("comment", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
    ]),
    "edge_cases_test": OrderedDict([
        ("pk", {"type": "BIGINT", "nullable": False, "spark": LongType}),
        ("large_int", {"type": "BIGINT", "nullable": True, "spark": LongType}),
        ("negative_int", {"type": "INT", "nullable": True, "spark": IntegerType}),
        ("high_precision_dec", {"type": "DECIMAL(38,18)", "nullable": True, "spark": lambda: DecimalType(38, 18)}),
        ("zero_dec", {"type": "DECIMAL(5,2)", "nullable": True, "spark": lambda: DecimalType(5, 2)}),
        ("empty_string", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
        ("unicode_string", {"type": "VARCHAR", "nullable": True, "spark": StringType}),
        ("timestamp_min", {"type": "TIMESTAMP", "nullable": True, "spark": TimestampType}),
        ("timestamp_max", {"type": "TIMESTAMP", "nullable": True, "spark": TimestampType}),
        ("bool_true", {"type": "BOOLEAN", "nullable": True, "spark": BooleanType}),
        ("bool_null", {"type": "BOOLEAN", "nullable": True, "spark": BooleanType}),
        ("floating_nan", {"type": "DOUBLE", "nullable": True, "spark": DoubleType}),
        ("floating_inf", {"type": "DOUBLE", "nullable": True, "spark": DoubleType}),
])
}