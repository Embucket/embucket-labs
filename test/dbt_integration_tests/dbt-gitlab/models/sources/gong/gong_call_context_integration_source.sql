WITH source AS (
  SELECT *
  FROM {{ source('gong','call_context_integration') }}
),

renamed AS (
  SELECT
    call_id::NUMBER                 AS call_id,
    object_id::STRING               AS object_id,
    name::STRING                    AS name,
    value::VARIANT                  AS value,
    systems::VARIANT                AS systems,
    object_type::STRING             AS object_type,
    timing::VARIANT                 AS timing,
    _fivetran_deleted::BOOLEAN      AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP     AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed