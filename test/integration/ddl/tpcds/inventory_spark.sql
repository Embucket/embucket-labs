-- Spark SQL DDL for inventory (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  inv_date_sk INT,
  inv_item_sk INT,
  inv_warehouse_sk INT,
  inv_quantity_on_hand INT
) USING iceberg;