use datafusion::arrow::datatypes::SchemaBuilder;
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    common::plan_err,
    error::Result,
};
use std::fs;
pub mod run;

pub use self::run::RunOpt;

pub mod convert;
pub use self::convert::ConvertOpt;

pub const TPCH_TABLES: &[&str] = &[
    "part", "supplier", "partsupp", "customer", "orders", "lineitem", "nation", "region",
];

/// The `.tbl` file contains a trailing column
#[must_use]
pub fn get_tbl_tpch_table_schema(table: &str) -> Schema {
    let mut schema = SchemaBuilder::from(get_tpch_table_schema(table).fields);
    schema.push(Field::new("__placeholder", DataType::Utf8, false));
    schema.finish()
}

/// Get the schema for the benchmarks derived from TPC-H
#[must_use]
pub fn get_tpch_table_schema(table: &str) -> Schema {
    // note that the schema intentionally uses signed integers so that any generated Parquet
    // files can also be used to benchmark tools that only support signed integers, such as
    // Apache Spark

    match table {
        "part" => Schema::new(vec![
            Field::new("p_partkey", DataType::Int64, false),
            Field::new("p_name", DataType::Utf8, false),
            Field::new("p_mfgr", DataType::Utf8, false),
            Field::new("p_brand", DataType::Utf8, false),
            Field::new("p_type", DataType::Utf8, false),
            Field::new("p_size", DataType::Int32, false),
            Field::new("p_container", DataType::Utf8, false),
            Field::new("p_retailprice", DataType::Decimal128(15, 2), false),
            Field::new("p_comment", DataType::Utf8, false),
        ]),

        "supplier" => Schema::new(vec![
            Field::new("s_suppkey", DataType::Int64, false),
            Field::new("s_name", DataType::Utf8, false),
            Field::new("s_address", DataType::Utf8, false),
            Field::new("s_nationkey", DataType::Int64, false),
            Field::new("s_phone", DataType::Utf8, false),
            Field::new("s_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("s_comment", DataType::Utf8, false),
        ]),

        "partsupp" => Schema::new(vec![
            Field::new("ps_partkey", DataType::Int64, false),
            Field::new("ps_suppkey", DataType::Int64, false),
            Field::new("ps_availqty", DataType::Int32, false),
            Field::new("ps_supplycost", DataType::Decimal128(15, 2), false),
            Field::new("ps_comment", DataType::Utf8, false),
        ]),

        "customer" => Schema::new(vec![
            Field::new("c_custkey", DataType::Int64, false),
            Field::new("c_name", DataType::Utf8, false),
            Field::new("c_address", DataType::Utf8, false),
            Field::new("c_nationkey", DataType::Int64, false),
            Field::new("c_phone", DataType::Utf8, false),
            Field::new("c_acctbal", DataType::Decimal128(15, 2), false),
            Field::new("c_mktsegment", DataType::Utf8, false),
            Field::new("c_comment", DataType::Utf8, false),
        ]),

        "orders" => Schema::new(vec![
            Field::new("o_orderkey", DataType::Int64, false),
            Field::new("o_custkey", DataType::Int64, false),
            Field::new("o_orderstatus", DataType::Utf8, false),
            Field::new("o_totalprice", DataType::Decimal128(15, 2), false),
            Field::new("o_orderdate", DataType::Date32, false),
            Field::new("o_orderpriority", DataType::Utf8, false),
            Field::new("o_clerk", DataType::Utf8, false),
            Field::new("o_shippriority", DataType::Int32, false),
            Field::new("o_comment", DataType::Utf8, false),
        ]),

        "lineitem" => Schema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_partkey", DataType::Int64, false),
            Field::new("l_suppkey", DataType::Int64, false),
            Field::new("l_linenumber", DataType::Int32, false),
            Field::new("l_quantity", DataType::Decimal128(15, 2), false),
            Field::new("l_extendedprice", DataType::Decimal128(15, 2), false),
            Field::new("l_discount", DataType::Decimal128(15, 2), false),
            Field::new("l_tax", DataType::Decimal128(15, 2), false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_linestatus", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_receiptdate", DataType::Date32, false),
            Field::new("l_shipinstruct", DataType::Utf8, false),
            Field::new("l_shipmode", DataType::Utf8, false),
            Field::new("l_comment", DataType::Utf8, false),
        ]),

        "nation" => Schema::new(vec![
            Field::new("n_nationkey", DataType::Int64, false),
            Field::new("n_name", DataType::Utf8, false),
            Field::new("n_regionkey", DataType::Int64, false),
            Field::new("n_comment", DataType::Utf8, false),
        ]),

        "region" => Schema::new(vec![
            Field::new("r_regionkey", DataType::Int64, false),
            Field::new("r_name", DataType::Utf8, false),
            Field::new("r_comment", DataType::Utf8, false),
        ]),

        _ => unimplemented!(),
    }
}

#[must_use]
#[allow(clippy::too_many_lines)]
pub fn get_tpch_table_sql(table: &str) -> Option<&'static str> {
    match table {
        "customer" => Some(
            "
            CREATE OR REPLACE TABLE customer (
              c_custkey BIGINT,
              c_name VARCHAR(25),
              c_address VARCHAR(40),
              c_nationkey INT,
              c_phone VARCHAR(15),
              c_acctbal DOUBLE,
              c_mktsegment VARCHAR(10),
              c_comment VARCHAR(117)
            );
        ",
        ),
        "lineitem" => Some(
            "
            CREATE OR REPLACE TABLE lineitem (
              l_orderkey BIGINT,
              l_partkey BIGINT,
              l_suppkey BIGINT,
              l_linenumber INT,
              l_quantity DOUBLE,
              l_extendedprice DOUBLE,
              l_discount DOUBLE,
              l_tax DOUBLE,
              l_returnflag VARCHAR(1),
              l_linestatus VARCHAR(1),
              l_shipdate DATE,
              l_commitdate DATE,
              l_receiptdate DATE,
              l_shipinstruct VARCHAR(25),
              l_shipmode VARCHAR(10),
              l_comment VARCHAR(44)
            );
        ",
        ),
        "nation" => Some(
            "
            CREATE OR REPLACE TABLE nation (
              n_nationkey INT,
              n_name VARCHAR(25),
              n_regionkey INT,
              n_comment VARCHAR(152)
            );
        ",
        ),

        "orders" => Some(
            "
            CREATE OR REPLACE TABLE orders (
              o_orderkey BIGINT,
              o_custkey BIGINT,
              o_orderstatus VARCHAR(1),
              o_totalprice DOUBLE,
              o_orderdate DATE,
              o_orderpriority VARCHAR(15),
              o_clerk VARCHAR(15),
              o_shippriority INT,
              o_comment VARCHAR(79)
            );
        ",
        ),

        "part" => Some(
            "
            CREATE OR REPLACE TABLE part (
              p_partkey BIGINT,
              p_name VARCHAR(55),
              p_mfgr VARCHAR(25),
              p_brand VARCHAR(10),
              p_type VARCHAR(25),
              p_size INT,
              p_container VARCHAR(10),
              p_retailprice DOUBLE,
              p_comment VARCHAR(23)
            );
        ",
        ),

        "partsupp" => Some(
            "
            CREATE OR REPLACE TABLE partsupp (
              ps_partkey BIGINT,
              ps_suppkey BIGINT,
              ps_availqty INT,
              ps_supplycost DOUBLE,
              ps_comment VARCHAR(199)
            );
        ",
        ),

        "region" => Some(
            "
            CREATE OR REPLACE TABLE region (
              r_regionkey INT,
              r_name VARCHAR(25),
              r_comment VARCHAR(152)
            );
        ",
        ),

        "supplier" => Some(
            "
            CREATE OR REPLACE TABLE supplier (
              s_suppkey BIGINT,
              s_name VARCHAR(25),
              s_address VARCHAR(40),
              s_nationkey INT,
              s_phone VARCHAR(15),
              s_acctbal DOUBLE,
              s_comment VARCHAR(101)
            );
        ",
        ),
        _ => None,
    }
}

/// Get the SQL statements from the specified query file
pub fn get_query_sql(query: usize) -> Result<Vec<String>> {
    if query > 0 && query < 23 {
        let possibilities = vec![
            format!("queries/tpch/q{query}.sql"),
            format!("benchmarks/queries/tpch/q{query}.sql"),
        ];
        let mut errors = vec![];
        for filename in possibilities {
            match fs::read_to_string(&filename) {
                Ok(contents) => {
                    return Ok(contents
                        .split(';')
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                        .map(std::string::ToString::to_string)
                        .collect());
                }
                Err(e) => errors.push(format!("{filename}: {e}")),
            }
        }
        plan_err!("invalid query. Could not find query: {:?}", errors)
    } else {
        plan_err!("invalid query. Expected value between 1 and 22")
    }
}

pub const QUERY_LIMIT: [Option<usize>; 22] = [
    None,
    Some(100),
    Some(10),
    None,
    None,
    None,
    None,
    None,
    None,
    Some(20),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
    Some(100),
    None,
    None,
    Some(100),
    None,
];
