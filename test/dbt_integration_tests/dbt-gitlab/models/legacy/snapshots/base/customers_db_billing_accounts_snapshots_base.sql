{{ config({
    "alias": "customers_db_billing_accounts_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'customers_db_billing_accounts_snapshots') }}

), renamed AS (

    SELECT 
      dbt_scd_id::VARCHAR                                     AS billing_account_snapshot_id,
      id::NUMBER                                              AS billing_account_id,
      zuora_account_id::VARCHAR                               AS zuora_account_id,
      zuora_account_name::VARCHAR                             AS zuora_account_name,
      salesforce_account_id::VARCHAR                          AS sfdc_account_id,
      created_at::TIMESTAMP                                   AS billing_account_created_at,
      updated_at::TIMESTAMP                                   AS billing_account_updated_at,
      dbt_valid_from::TIMESTAMP                             AS valid_from,
      dbt_valid_to::TIMESTAMP                               AS valid_to
    FROM source

)

SELECT *
FROM renamed
