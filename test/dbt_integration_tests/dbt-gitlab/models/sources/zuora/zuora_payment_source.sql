-- depends_on: {{ ref('zuora_excluded_accounts') }}

{{ config(
    materialized="view",
    tags=["mnpi"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('zuora', 'payment') }}

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
