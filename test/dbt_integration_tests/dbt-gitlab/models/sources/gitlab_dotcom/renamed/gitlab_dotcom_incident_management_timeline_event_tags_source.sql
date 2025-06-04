    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_timeline_event_tags_dedupe_source') }}
  
),
renamed AS (

    SELECT
      id::NUMBER             AS event_tag_id,
      created_at::TIMESTAMP  AS created_at,
      updated_at::TIMESTAMP  AS updated_at,
      project_id::NUMBER     AS project_id
    FROM source

)

SELECT *
FROM renamed
