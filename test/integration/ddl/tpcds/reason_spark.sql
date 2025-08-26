-- Spark SQL DDL for reason (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  r_reason_sk INT,
  r_reason_id STRING,
  r_reason_desc STRING
) USING iceberg;