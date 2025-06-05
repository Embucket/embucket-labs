
  create or replace   view EMBUCKET.zuora.zuora_payment_source
  
   as (
    -- depends_on: EMBUCKET.seed_finance.zuora_excluded_accounts



WITH source AS (

  SELECT *
  FROM EMBUCKET.zuora_stitch.payment

),

renamed AS (

  SELECT
    -- primary key 
    id            AS payment_id,

    -- keys
    paymentnumber AS payment_number,
    accountid     AS account_id,

    -- payment dates
    effectivedate AS payment_date,


    -- additive fields
    status        AS payment_status,
    type          AS payment_type,
    amount        AS payment_amount


  FROM source

)

SELECT *
FROM renamed
  );

