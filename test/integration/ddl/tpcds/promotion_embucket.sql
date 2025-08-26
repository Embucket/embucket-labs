-- Snowflake-like DDL for promotion
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  p_promo_sk INTEGER,
  p_promo_id VARCHAR,
  p_start_date_sk INTEGER,
  p_end_date_sk INTEGER,
  p_item_sk INTEGER,
  p_cost DECIMAL(15,2),
  p_response_target INTEGER,
  p_promo_name VARCHAR,
  p_channel_dmail VARCHAR,
  p_channel_email VARCHAR,
  p_channel_catalog VARCHAR,
  p_channel_tv VARCHAR,
  p_channel_radio VARCHAR,
  p_channel_press VARCHAR,
  p_channel_event VARCHAR,
  p_channel_demo VARCHAR,
  p_channel_details VARCHAR,
  p_purpose VARCHAR,
  p_discount_active VARCHAR
);
