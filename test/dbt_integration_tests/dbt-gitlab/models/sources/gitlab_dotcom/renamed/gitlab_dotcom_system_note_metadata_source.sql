WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_system_note_metadata_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                           AS system_note_metadata_id,
      note_id::NUMBER                      AS note_id,
      commit_count::NUMBER                 AS commit_count,
      action::VARCHAR                       AS action_type,
      description_version_id::NUMBER       AS description_version_id,
      created_at::TIMESTAMP                 AS created_at,
      updated_at::TIMESTAMP                 AS updated_at

    FROM source

)

SELECT  *
FROM renamed
