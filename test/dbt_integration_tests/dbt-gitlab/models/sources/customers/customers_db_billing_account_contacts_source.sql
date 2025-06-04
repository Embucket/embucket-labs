WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_billing_account_contacts') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                                              AS billing_account_contact_id,
      work_email::VARCHAR                                     AS work_email,
      zuora_contact_id::VARCHAR                                AS zuora_contact_id,
      zuora_account_id::VARCHAR                               AS zuora_account_id,
      created_at::TIMESTAMP                                   AS billing_account_contact_created_at,
      updated_at::TIMESTAMP                                   AS billing_account_contact_updated_at
    FROM source

)

SELECT
  *
FROM renamed
