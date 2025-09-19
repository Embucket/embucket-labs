-- Spark SQL DDL for ship_mode (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  sm_ship_mode_sk INT,
  sm_ship_mode_id STRING,
  sm_type STRING,
  sm_code STRING,
  sm_carrier STRING,
  sm_contract STRING
) USING iceberg;