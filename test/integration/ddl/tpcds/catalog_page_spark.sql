-- Spark SQL DDL for catalog_page (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  cp_catalog_page_sk INT,
  cp_catalog_page_id STRING,
  cp_start_date_sk INT,
  cp_end_date_sk INT,
  cp_department STRING,
  cp_catalog_number INT,
  cp_catalog_page_number INT,
  cp_description STRING,
  cp_type STRING
) USING iceberg;