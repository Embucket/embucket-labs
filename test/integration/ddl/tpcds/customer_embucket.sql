-- Snowflake-like DDL for customer
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  c_customer_sk INTEGER,
  c_customer_id VARCHAR,
  c_current_cdemo_sk INTEGER,
  c_current_hdemo_sk INTEGER,
  c_current_addr_sk INTEGER,
  c_first_shipto_date_sk INTEGER,
  c_first_sales_date_sk INTEGER,
  c_salutation VARCHAR,
  c_first_name VARCHAR,
  c_last_name VARCHAR,
  c_preferred_cust_flag VARCHAR,
  c_birth_day INTEGER,
  c_birth_month INTEGER,
  c_birth_year INTEGER,
  c_birth_country VARCHAR,
  c_login VARCHAR,
  c_email_address VARCHAR,
  c_last_review_date_sk INTEGER
);
