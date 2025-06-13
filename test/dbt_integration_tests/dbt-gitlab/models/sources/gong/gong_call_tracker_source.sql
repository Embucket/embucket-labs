WITH source AS (
  SELECT *
  FROM {{ source('gong','call_tracker') }}
),

renamed AS (
  SELECT
    id::NUMBER                      AS tracker_id,
    call_id::NUMBER                 AS call_id,
    start_time::NUMBER              AS start_time,
    phrase::STRING                  AS phrase,
    speaker_id::STRING              AS speaker_id,
    name::STRING                    AS tracker_name,
    type::STRING                    AS type,
    _fivetran_deleted::BOOLEAN      AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP     AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed