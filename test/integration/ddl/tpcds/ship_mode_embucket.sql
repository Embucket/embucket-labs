-- Snowflake-like DDL for ship_mode
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  sm_ship_mode_sk INTEGER,
  sm_ship_mode_id VARCHAR,
  sm_type VARCHAR,
  sm_code VARCHAR,
  sm_carrier VARCHAR,
  sm_contract VARCHAR
);
