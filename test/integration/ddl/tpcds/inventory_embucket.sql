-- Snowflake-like DDL for inventory
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  inv_date_sk INTEGER,
  inv_item_sk INTEGER,
  inv_warehouse_sk INTEGER,
  inv_quantity_on_hand INTEGER
);
