WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_billing_accounts') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                                              AS billing_account_id,
      zuora_account_id::VARCHAR                               AS zuora_account_id,
      zuora_account_name::VARCHAR                             AS zuora_account_name,
      salesforce_account_id::VARCHAR                          AS sfdc_account_id,
      created_at::TIMESTAMP                                   AS billing_account_created_at,
      updated_at::TIMESTAMP                                   AS billing_account_updated_at
    FROM source

)

SELECT
  *
FROM renamed
