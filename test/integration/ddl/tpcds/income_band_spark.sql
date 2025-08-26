-- Spark SQL DDL for income_band (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  ib_income_band_sk INT,
  ib_lower_bound INT,
  ib_upper_bound INT
) USING iceberg;