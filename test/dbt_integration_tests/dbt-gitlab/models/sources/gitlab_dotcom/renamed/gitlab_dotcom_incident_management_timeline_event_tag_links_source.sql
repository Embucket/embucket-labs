    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_timeline_event_tag_links_dedupe_source') }}
  
),
renamed AS (

    SELECT
      id::NUMBER                     AS event_tag_link_id,
      timeline_event_id::NUMBER      AS timeline_event_id,
      timeline_event_tag_id::NUMBER  AS timeline_event_tag_id,
      created_at::TIMESTAMP          AS created_at,
    FROM source

)

SELECT *
FROM renamed
