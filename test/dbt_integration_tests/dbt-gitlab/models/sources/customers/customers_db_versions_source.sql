{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('customers', 'customers_db_versions') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER             AS version_id,
      item_id::NUMBER        AS item_id,
      transaction_id::NUMBER AS transaction_id,
      created_at::TIMESTAMP   AS created_at,
      event::VARCHAR          AS event,
      item_type::VARCHAR      AS item_type,
      object::VARCHAR         AS object,
      object_changes::VARCHAR AS object_changes,
      whodunnit::VARCHAR      AS whodunnit
    FROM source  

)

SELECT *
FROM renamed
