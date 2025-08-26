-- Spark SQL DDL for NYC Taxi Yellow Trips (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  vendorid INT,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance DOUBLE,
  ratecodeid INT,
  store_and_fwd_flag STRING,
  pulocationid INT,
  dolocationid INT,
  payment_type INT,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount DOUBLE,
  congestion_surcharge DOUBLE,
  airport_fee DOUBLE,
  cbd_congestion_fee DOUBLE
) USING iceberg;

