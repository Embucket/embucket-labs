-- Snowflake-like DDL for TPC-H SUPPLIER
CREATE OR REPLACE TABLE supplier (
  s_suppkey BIGINT,
  s_name VARCHAR(25),
  s_address VARCHAR(40),
  s_nationkey INT,
  s_phone VARCHAR(15),
  s_acctbal DOUBLE,
  s_comment VARCHAR(101)
);