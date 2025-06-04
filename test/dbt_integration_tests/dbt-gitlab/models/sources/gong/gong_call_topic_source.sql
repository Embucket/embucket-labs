WITH source AS (
  SELECT *
  FROM {{ source('gong','call_topic') }}
),

renamed AS (
  SELECT
    call_id::NUMBER                 AS call_id,
    duration::NUMBER                AS duration_seconds,
    name::STRING                    AS topic_name,
    _fivetran_deleted::BOOLEAN      AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP     AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed