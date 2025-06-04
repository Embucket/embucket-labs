-- depends_on: {{ ref('zuora_excluded_accounts') }}

{{ config(
    materialized="view",
    tags=["mnpi"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'invoice_item_adjustment') }}

),

renamed AS (

  SELECT
    -- primary key 
    id                 AS invoice_item_adjustment_id,

    -- keys
    adjustmentnumber   AS invoice_item_adjustment_number,
    accountid          AS account_id,
    invoiceid          AS invoice_id,
    accountingperiodid AS accounting_period_id,


    -- invoice item adjustment dates
    adjustmentdate     AS invoice_item_adjustment_date,


    -- additive fields
    amount             AS invoice_item_adjustment_amount,
    status             AS invoice_item_adjustment_status,
    type               AS invoice_item_adjustment_type


  FROM source

)

SELECT *
FROM renamed
