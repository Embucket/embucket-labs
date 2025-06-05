
  create or replace   view EMBUCKET.zuora.zuora_credit_balance_adjustment_source
  
   as (
    -- depends_on: EMBUCKET.seed_finance.zuora_excluded_accounts



WITH source AS (

  SELECT *
  FROM EMBUCKET.zuora_stitch.creditbalanceadjustment

),

renamed AS (

  SELECT
    -- primary key 
    id                 AS credit_balance_adjustment_id,

    -- keys
    number             AS credit_balance_adjustment_number,
    accountid          AS account_id,
    invoiceid          AS invoice_id,
    accountingperiodid AS accounting_period_id,


    -- credit balance adjustment dates
    adjustmentdate     AS credit_balance_adjustment_date,

    -- additive fields
    amount             AS credit_balance_adjustment_amount,
    status             AS credit_balance_adjustment_status,
    type               AS credit_balance_adjustment_type

  FROM source

)

SELECT *
FROM renamed
  );

