-- Snowflake-like DDL for customer_demographics
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  cd_demo_sk INTEGER,
  cd_gender VARCHAR,
  cd_marital_status VARCHAR,
  cd_education_status VARCHAR,
  cd_purchase_estimate INTEGER,
  cd_credit_rating VARCHAR,
  cd_dep_count INTEGER,
  cd_dep_employed_count INTEGER,
  cd_dep_college_count INTEGER
);
