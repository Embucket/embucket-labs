-- Snowflake-like DDL for catalog_page
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  cp_catalog_page_sk INTEGER,
  cp_catalog_page_id VARCHAR,
  cp_start_date_sk INTEGER,
  cp_end_date_sk INTEGER,
  cp_department VARCHAR,
  cp_catalog_number INTEGER,
  cp_catalog_page_number INTEGER,
  cp_description VARCHAR,
  cp_type VARCHAR
);
