

WITH resource_label_events AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_resource_label_events') }}
  
)

, epics AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_epics_source') }}

)

, issues AS (

    SELECT
      prep_issue.*,
      prep_namespace.namespace_id
    FROM {{ ref('prep_issue') }}
    LEFT JOIN  {{ ref('prep_namespace') }}
      ON prep_issue.dim_namespace_sk = prep_namespace.dim_namespace_sk
    WHERE prep_namespace.is_currently_valid = TRUE

)

, mrs AS (

    SELECT
      prep_merge_request.*,
      prep_namespace.namespace_id
    FROM {{ ref('prep_merge_request') }}
    LEFT JOIN  {{ ref('prep_namespace') }}
      ON prep_merge_request.dim_namespace_sk = prep_namespace.dim_namespace_sk
    WHERE prep_namespace.is_currently_valid = TRUE

)

, joined AS (

    SELECT 
      resource_label_events.*,
      COALESCE(epics.group_id,
                issues.namespace_id,
                mrs.namespace_id) AS namespace_id
    FROM resource_label_events
    LEFT JOIN epics
      ON resource_label_events.epic_id = epics.epic_id
    LEFT JOIN issues
      ON resource_label_events.issue_id = issues.issue_id
    LEFT JOIN mrs
      ON resource_label_events.merge_request_id = mrs.merge_request_id

)

SELECT *
FROM joined
