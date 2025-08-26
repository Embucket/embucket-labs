CREATE TABLE {{TABLE_FQN}} (
  VendorID INTEGER,
  lpep_pickup_datetime TIMESTAMP,
  lpep_dropoff_datetime TIMESTAMP,
  store_and_fwd_flag STRING,
  RatecodeID INTEGER,
  PULocationID INTEGER,
  DOLocationID INTEGER,
  passenger_count INTEGER,
  trip_distance DOUBLE,
  fare_amount DOUBLE,
  extra DOUBLE,
  mta_tax DOUBLE,
  tip_amount DOUBLE,
  tolls_amount DOUBLE,
  ehail_fee DOUBLE,
  improvement_surcharge DOUBLE,
  total_amount DOUBLE,
  payment_type INTEGER,
  trip_type INTEGER,
  congestion_surcharge DOUBLE,
  cbd_congestion_fee DOUBLE
)
USING iceberg;