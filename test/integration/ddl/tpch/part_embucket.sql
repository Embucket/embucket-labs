-- Snowflake-like DDL for TPC-H PART
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
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