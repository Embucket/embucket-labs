WITH source AS (
  SELECT *
  FROM {{ source('gong','transcript') }}
),

renamed AS (
  SELECT
    call_id::NUMBER               AS call_id,
    index::NUMBER                 AS index,
    workspace_id::STRING          AS workspace_id,
    sentence::STRING              AS sentence,
    speaker_id::STRING            AS speaker_id,
    topic::STRING                 AS topic,
    _fivetran_deleted::BOOLEAN    AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP   AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed