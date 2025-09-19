-- Snowflake-like DDL for store_returns
CREATE OR REPLACE TABLE {{TABLE_FQN}} (
  sr_returned_date_sk INTEGER,
  sr_return_time_sk INTEGER,
  sr_item_sk INTEGER,
  sr_customer_sk INTEGER,
  sr_cdemo_sk INTEGER,
  sr_hdemo_sk INTEGER,
  sr_addr_sk INTEGER,
  sr_store_sk INTEGER,
  sr_reason_sk INTEGER,
  sr_ticket_number INTEGER,
  sr_return_quantity INTEGER,
  sr_return_amt DECIMAL(7,2),
  sr_return_tax DECIMAL(7,2),
  sr_return_amt_inc_tax DECIMAL(7,2),
  sr_fee DECIMAL(7,2),
  sr_return_ship_cost DECIMAL(7,2),
  sr_refunded_cash DECIMAL(7,2),
  sr_reversed_charge DECIMAL(7,2),
  sr_store_credit DECIMAL(7,2),
  sr_net_loss DECIMAL(7,2)
);
