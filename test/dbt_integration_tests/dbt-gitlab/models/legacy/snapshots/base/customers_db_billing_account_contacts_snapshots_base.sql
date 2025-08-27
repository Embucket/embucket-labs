{{ config({
    "alias": "customers_db_billing_account_contacts_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'customers_db_billing_account_contacts_snapshots') }}

), renamed AS (

    SELECT 
      dbt_scd_id::VARCHAR                                     AS billing_account_contact_snapshot_id,
      id::NUMBER                                              AS billing_account_contact_id,
      work_email::VARCHAR                                     AS work_email,
      zuora_account_id::VARCHAR                               AS zuora_account_id,
      zuora_contact_id::VARCHAR                               AS zuora_contact_id,
      created_at::TIMESTAMP                                   AS billing_account_contact_created_at,
      updated_at::TIMESTAMP                                   AS billing_account_contact_updated_at,
      dbt_valid_from::TIMESTAMP                             AS valid_from,
      dbt_valid_to::TIMESTAMP                               AS valid_to
    FROM source

)

SELECT *
FROM renamed
