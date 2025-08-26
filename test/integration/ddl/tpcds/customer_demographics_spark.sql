-- Spark SQL DDL for customer_demographics (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  cd_demo_sk INT,
  cd_gender STRING,
  cd_marital_status STRING,
  cd_education_status STRING,
  cd_purchase_estimate INT,
  cd_credit_rating STRING,
  cd_dep_count INT,
  cd_dep_employed_count INT,
  cd_dep_college_count INT
) USING iceberg;