-- Spark SQL DDL for promotion (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  p_promo_sk INT,
  p_promo_id STRING,
  p_start_date_sk INT,
  p_end_date_sk INT,
  p_item_sk INT,
  p_cost DECIMAL(15,2),
  p_response_target INT,
  p_promo_name STRING,
  p_channel_dmail STRING,
  p_channel_email STRING,
  p_channel_catalog STRING,
  p_channel_tv STRING,
  p_channel_radio STRING,
  p_channel_press STRING,
  p_channel_event STRING,
  p_channel_demo STRING,
  p_channel_details STRING,
  p_purpose STRING,
  p_discount_active STRING
) USING iceberg;