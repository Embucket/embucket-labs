-- Spark SQL DDL for warehouse (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  w_warehouse_sk INT,
  w_warehouse_id STRING,
  w_warehouse_name STRING,
  w_warehouse_sq_ft INT,
  w_street_number STRING,
  w_street_name STRING,
  w_street_type STRING,
  w_suite_number STRING,
  w_city STRING,
  w_county STRING,
  w_state STRING,
  w_zip STRING,
  w_country STRING,
  w_gmt_offset DECIMAL(5,2)
) USING iceberg;