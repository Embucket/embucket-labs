WITH source AS (
  SELECT *
  FROM RAW.gong.call_interaction
),

renamed AS (
  SELECT
    call_id::NUMBER                 AS call_id,
    name::STRING                    AS name,
    value::VARIANT                  AS value,
    _fivetran_deleted::BOOLEAN      AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP     AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed