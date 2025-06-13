    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_incident_management_issuable_escalation_statuses_dedupe_source') }}
  
),
renamed AS (

    SELECT
      id::NUMBER                        AS status_id,
      created_at::TIMESTAMP             AS created_at,
      updated_at::TIMESTAMP             AS updated_at,
      issue_id::NUMBER                  AS issue_id,
      policy_id::NUMBER                 AS policy_id,
      escalations_started_at::TIMESTAMP AS escalations_started_at,
      resolved_at::TIMESTAMP            AS resolved_at,
      status::NUMBER                    AS status
    FROM source

)

SELECT *
FROM renamed
