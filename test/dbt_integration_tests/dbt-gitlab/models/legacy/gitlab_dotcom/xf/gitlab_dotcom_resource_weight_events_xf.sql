

WITH resource_weight_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_weight_events') }}
  
)

, issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}
    
)

, joined AS (

    SELECT 
      resource_weight_events.*,
      issues.project_id
    FROM resource_weight_events
    LEFT JOIN issues
      ON resource_weight_events.issue_id = issues.issue_id

)

SELECT *
FROM joined
