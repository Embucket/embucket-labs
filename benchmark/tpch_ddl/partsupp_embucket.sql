-- Snowflake-like DDL for TPC-H PARTSUPP
CREATE OR REPLACE TABLE partsupp (
  ps_partkey BIGINT,
  ps_suppkey BIGINT,
  ps_availqty INT,
  ps_supplycost DOUBLE,
  ps_comment VARCHAR(199)
);