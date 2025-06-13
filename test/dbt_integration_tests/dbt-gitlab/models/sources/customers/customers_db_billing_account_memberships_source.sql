WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_billing_account_memberships') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) = 1

), renamed AS (

    SELECT DISTINCT
      id::NUMBER                                              AS billing_account_membership_id,
      customer_id::NUMBER                                     AS customer_id,
      billing_account_id::NUMBER                              AS billing_account_id,
      created_at::TIMESTAMP                                   AS billing_account_membership_created_at,
      updated_at::TIMESTAMP                                   AS billing_account_membership_updated_at
    FROM source

)

SELECT
  *
FROM renamed
