WITH source AS (
  SELECT *
  FROM {{ source('gong','call_content_action_item') }}
),

renamed AS (
  SELECT
    call_id::NUMBER                 AS call_id,
    snippet_end_time::NUMBER        AS snippet_end_time,
    snippet_start_time::NUMBER      AS snippet_start_time,
    speaker_id::STRING              AS speaker_id,
    snippet::STRING                 AS snippet,
    _fivetran_deleted::BOOLEAN      AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP     AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed