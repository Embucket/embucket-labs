WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'payment_method') }}

), renamed AS(

    SELECT 
      id                          AS payment_method_id,
      -- keys
      accountid                   AS account_id,

      -- payment method info
      type                        AS payment_method_type,
      subtype                     AS payment_method_subtype,

      -- metadata
      createdbyid                 AS created_by_id,
      createddate                 AS created_date,
      updatedbyid                 AS updated_by_id,
      updateddate                 AS updated_date,
      deleted                     AS is_deleted

    FROM source

)

SELECT *
FROM renamed
