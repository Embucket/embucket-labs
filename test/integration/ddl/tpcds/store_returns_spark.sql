-- Spark SQL DDL for store_returns (Iceberg table)
CREATE TABLE {{TABLE_FQN}} (
  sr_returned_date_sk INT,
  sr_return_time_sk INT,
  sr_item_sk INT,
  sr_customer_sk INT,
  sr_cdemo_sk INT,
  sr_hdemo_sk INT,
  sr_addr_sk INT,
  sr_store_sk INT,
  sr_reason_sk INT,
  sr_ticket_number INT,
  sr_return_quantity INT,
  sr_return_amt DECIMAL(7,2),
  sr_return_tax DECIMAL(7,2),
  sr_return_amt_inc_tax DECIMAL(7,2),
  sr_fee DECIMAL(7,2),
  sr_return_ship_cost DECIMAL(7,2),
  sr_refunded_cash DECIMAL(7,2),
  sr_reversed_charge DECIMAL(7,2),
  sr_store_credit DECIMAL(7,2),
  sr_net_loss DECIMAL(7,2)
) USING iceberg;