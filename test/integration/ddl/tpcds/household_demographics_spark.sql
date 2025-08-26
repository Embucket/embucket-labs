-- Spark SQL DDL for household_demographics (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  hd_demo_sk INT,
  hd_income_band_sk INT,
  hd_buy_potential STRING,
  hd_dep_count INT,
  hd_vehicle_count INT
) USING iceberg;