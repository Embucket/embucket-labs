WITH source AS (
  SELECT *
  FROM {{ source('gong','tracker_language') }}
),

renamed AS (
  SELECT
    tracker_id::NUMBER                AS tracker_id,
    tracker_language::STRING          AS tracker_language,
    include_related_forms::BOOLEAN    AS include_related_forms,
    _fivetran_deleted::BOOLEAN        AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP       AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed