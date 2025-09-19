-- Spark SQL DDL for customer (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  c_customer_sk INT,
  c_customer_id STRING,
  c_current_cdemo_sk INT,
  c_current_hdemo_sk INT,
  c_current_addr_sk INT,
  c_first_shipto_date_sk INT,
  c_first_sales_date_sk INT,
  c_salutation STRING,
  c_first_name STRING,
  c_last_name STRING,
  c_preferred_cust_flag STRING,
  c_birth_day INT,
  c_birth_month INT,
  c_birth_year INT,
  c_birth_country STRING,
  c_login STRING,
  c_email_address STRING,
  c_last_review_date_sk INT
) USING iceberg;