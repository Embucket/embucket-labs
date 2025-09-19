-- Spark SQL DDL for time_dim (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  t_time_sk INT,
  t_time_id STRING,
  t_time INT,
  t_hour INT,
  t_minute INT,
  t_second INT,
  t_am_pm STRING,
  t_shift STRING,
  t_sub_shift STRING,
  t_meal_time STRING
) USING iceberg;