    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_integrations_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                             AS integration_id,
      type_new::VARCHAR                      AS integration_type,
      project_id::NUMBER                     AS project_id,
      created_at::TIMESTAMP                  AS created_at,
      updated_at::TIMESTAMP                  AS updated_at,
      true::BOOLEAN                        AS is_active,
      true::BOOLEAN                      AS integration_template,
      true::BOOLEAN                   AS has_push_events,
      true::BOOLEAN                 AS has_issues_events,
      true::BOOLEAN         AS has_merge_requests_events,
      true::BOOLEAN               AS has_tag_push_events,
      true::BOOLEAN                   AS has_note_events,
      category::VARCHAR                      AS integration_category,
      true::BOOLEAN              AS has_wiki_page_events,
      true::BOOLEAN               AS has_pipeline_events,
      true::BOOLEAN    AS has_confidential_issues_events,
      true::BOOLEAN                 AS has_commit_events,
      true::BOOLEAN                    AS has_job_events,
      true::BOOLEAN      AS has_confidential_note_events,
      true::BOOLEAN             AS has_deployment_events,
      true::BOOLEAN      AS is_comment_on_event_enabled,
      group_id::NUMBER                       AS group_id,
      inherit_from_id::NUMBER                AS inherit_from_id
    FROM source

)

SELECT *
FROM renamed
