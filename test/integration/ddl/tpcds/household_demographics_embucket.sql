-- Snowflake-like DDL for household_demographics
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  hd_demo_sk INTEGER,
  hd_income_band_sk INTEGER,
  hd_buy_potential VARCHAR,
  hd_dep_count INTEGER,
  hd_vehicle_count INTEGER
);
