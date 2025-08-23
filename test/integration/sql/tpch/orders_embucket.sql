-- Snowflake-like DDL for TPC-H ORDERS
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
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

