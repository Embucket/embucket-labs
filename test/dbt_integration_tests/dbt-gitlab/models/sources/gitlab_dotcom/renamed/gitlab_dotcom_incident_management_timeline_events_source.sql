    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_timeline_events_dedupe_source') }}
  
),
renamed AS (

    SELECT
      id::NUMBER                        AS id,
      created_at::TIMESTAMP             AS created_at,
      updated_at::TIMESTAMP             AS updated_at,
      occurred_at::TIMESTAMP            AS occurred_at,
      project_id::NUMBER                AS project_id,
      author_id::NUMBER                 AS author_id,
      issue_id::NUMBER                  AS issue_id,
      updated_by_user_id::NUMBER        AS updated_by_user_id,
      promoted_from_note_id::NUMBER     AS promoted_from_note_id,
      cached_markdown_version::NUMBER   AS cached_markdown_version,
      editable::BOOLEAN                 AS editable
    FROM source

)

SELECT *
FROM renamed
