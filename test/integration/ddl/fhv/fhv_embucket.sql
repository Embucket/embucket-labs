-- Snowflake-like DDL for NYC Taxi FHV Trips
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  dispatching_base_num VARCHAR(255),
  pickup_datetime TIMESTAMP,
  dropOff_datetime TIMESTAMP,
  PUlocationID INT,
  DOlocationID INT,
  SR_Flag INT,
  Affiliated_base_number VARCHAR(255)
);