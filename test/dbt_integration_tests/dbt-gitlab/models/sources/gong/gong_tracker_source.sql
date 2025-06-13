WITH source AS (
  SELECT *
  FROM {{ source('gong','tracker') }}
),

renamed AS (
  SELECT
    tracker_id::NUMBER            AS tracker_id,
    affiliation::STRING           AS affiliation,
    created::TIMESTAMP            AS created_at,
    tracker_name::STRING          AS tracker_name,
    part_of_question::BOOLEAN     AS part_of_question,
    said_at_interval::NUMBER      AS said_at_interval,
    workspace_id::STRING          AS workspace_id,
    creator_user_id::NUMBER       AS creator_user_id,
    said_at::TIMESTAMP            AS said_at,
    updated::TIMESTAMP            AS updated_at,
    filter_query::STRING          AS filter_query,
    updater_user_id::NUMBER       AS updater_user_id,
    _fivetran_deleted::BOOLEAN    AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP   AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed