-- Spark SQL DDL for NYC Taxi FHV Trips (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  dispatching_base_num STRING,
  pickup_datetime TIMESTAMP,
  dropOff_datetime TIMESTAMP,
  PUlocationID INT,
  DOlocationID INT,
  SR_Flag INT,
  Affiliated_base_number STRING
) USING iceberg;