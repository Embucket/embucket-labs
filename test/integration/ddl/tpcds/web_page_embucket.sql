-- Snowflake-like DDL for web_page
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  wp_web_page_sk INTEGER,
  wp_web_page_id VARCHAR,
  wp_rec_start_date DATE,
  wp_rec_end_date DATE,
  wp_creation_date_sk INTEGER,
  wp_access_date_sk INTEGER,
  wp_autogen_flag VARCHAR,
  wp_customer_sk INTEGER,
  wp_url VARCHAR,
  wp_type VARCHAR,
  wp_char_count INTEGER,
  wp_link_count INTEGER,
  wp_image_count INTEGER,
  wp_max_ad_count INTEGER
);
