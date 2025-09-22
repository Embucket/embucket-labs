-- Snowflake-like DDL for TPC-H CUSTOMER
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