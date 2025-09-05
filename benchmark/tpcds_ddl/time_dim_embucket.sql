CREATE OR REPLACE TABLE time_dim (
  t_time_sk INTEGER,
  t_time_id VARCHAR,
  t_time INTEGER,
  t_hour INTEGER,
  t_minute INTEGER,
  t_second INTEGER,
  t_am_pm VARCHAR,
  t_shift VARCHAR,
  t_sub_shift VARCHAR,
  t_meal_time VARCHAR
);
