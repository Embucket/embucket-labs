-- Spark SQL DDL for web_page (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  wp_web_page_sk INT,
  wp_web_page_id STRING,
  wp_rec_start_date DATE,
  wp_rec_end_date DATE,
  wp_creation_date_sk INT,
  wp_access_date_sk INT,
  wp_autogen_flag STRING,
  wp_customer_sk INT,
  wp_url STRING,
  wp_type STRING,
  wp_char_count INT,
  wp_link_count INT,
  wp_image_count INT,
  wp_max_ad_count INT
) USING iceberg;