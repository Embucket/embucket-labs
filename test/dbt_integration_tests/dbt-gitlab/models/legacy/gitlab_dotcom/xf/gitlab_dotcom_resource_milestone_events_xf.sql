

WITH gitlab_dotcom_resource_milestone_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_milestone_events') }}
  
)

, issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}

)

, mrs AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_merge_requests_source') }}

)

, joined AS (

    SELECT 
      gitlab_dotcom_resource_milestone_events.*,
      COALESCE(issues.project_id,
                mrs.project_id) AS project_id
    FROM gitlab_dotcom_resource_milestone_events
    LEFT JOIN issues
      ON gitlab_dotcom_resource_milestone_events.issue_id = issues.issue_id
    LEFT JOIN mrs
      ON gitlab_dotcom_resource_milestone_events.merge_request_id = mrs.merge_request_id

)

SELECT *
FROM joined
