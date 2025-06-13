{{ config(
    materialized="view",
    tags=["mnpi"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'booking_transaction') }}

),

renamed AS (

  SELECT
    -- primary key 
    id                 AS booking_transaction_id,

    -- keys
    rateplanchargeid   AS rate_plan_charge_id,

    -- additive fields
    listprice             AS list_price

  FROM source

)

SELECT *
FROM renamed