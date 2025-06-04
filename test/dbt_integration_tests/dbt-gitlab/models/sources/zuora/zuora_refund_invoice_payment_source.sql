-- depends_on: {{ ref('zuora_excluded_accounts') }}

{{ config(
    materialized="view",
    tags=["mnpi"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'refund_invoice_payment') }}

),

renamed AS (

  SELECT
    -- primary key 
    id                 AS refund_invoice_payment_id,

    -- keys
    invoiceid          AS invoice_id,
    refundid           AS refund_id,
    paymentid          AS payment_id,
    accountid          AS account_id,
    accountingperiodid AS accounting_period_id,


    -- refund invoice payment dates
    createddate        AS refund_invoice_payment_date,


    -- additive fields
    refundamount       AS refund_invoice_payment_amount



  FROM source

)

SELECT *
FROM renamed
