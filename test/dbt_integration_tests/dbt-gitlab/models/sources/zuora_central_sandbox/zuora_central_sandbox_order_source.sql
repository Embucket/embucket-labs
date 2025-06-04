WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'order') }}

), renamed AS (

    SELECT

      id                                    AS order_id,

      description                           AS description,
      order_date                            AS order_date,
      order_number                          AS order_number,
      state                                 AS state,
      status                                AS status,

      sold_to_contact_id                    AS sold_to_contact_id,


      account_id                            AS account_id,
      bill_to_contact_id                    AS bill_to_contact_id,
      default_payment_method_id             AS default_payment_method_id,

      -- metadata
      updated_by_id                         AS updated_by_id,
      updated_date                          AS updated_date,
      created_by_id                         AS created_by_id,
      created_date                          AS created_date,
      created_by_migration                  AS created_by_migration,

      _FIVETRAN_DELETED                     AS is_deleted

    FROM source

)

SELECT *
FROM renamed
