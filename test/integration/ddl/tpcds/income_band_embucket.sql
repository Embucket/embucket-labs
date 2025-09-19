-- Snowflake-like DDL for income_band
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  ib_income_band_sk INTEGER,
  ib_lower_bound INTEGER,
  ib_upper_bound INTEGER
);
