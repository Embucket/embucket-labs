WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notification_settings_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                      AS notification_settings_id,
      user_id::NUMBER                                 AS user_id,
      source_id::NUMBER                               AS source_id,
      created_at::TIMESTAMP                           AS created_at,
      updated_at::TIMESTAMP                           AS updated_at,
      source_type::VARCHAR                            AS source_type,
      level::NUMBER                                   AS level,
      new_note::BOOLEAN                               AS new_note,
      new_issue::BOOLEAN                              AS new_issue,
      reopen_issue::BOOLEAN                           AS reopen_issue,
      close_issue::BOOLEAN                            AS close_issue,
      reassign_issue::BOOLEAN                         AS reassign_issue,
      new_merge_request::BOOLEAN                      AS new_merge_request,
      reopen_merge_request::BOOLEAN                   AS reopen_merge_request,
      close_merge_request::BOOLEAN                    AS close_merge_request,
      reassign_merge_request::BOOLEAN                 AS reassign_merge_request,
      merge_merge_request::BOOLEAN                    AS merge_merge_request,
      failed_pipeline::BOOLEAN                        AS failed_pipeline,
      success_pipeline::BOOLEAN                       AS success_pipeline,
      push_to_merge_request::BOOLEAN                  AS push_to_merge_request,
      issue_due::BOOLEAN                              AS issue_due,
      new_epic::BOOLEAN                               AS new_epic,
      fixed_pipeline::BOOLEAN                         AS fixed_pipeline,
      new_release::BOOLEAN                            AS new_release,
      moved_project::BOOLEAN                          AS moved_project,
      change_reviewer_merge_request::BOOLEAN          AS change_reviewer_merge_request,
      merge_when_pipeline_succeeds::BOOLEAN           AS merge_when_pipeline_succeeds

    FROM source

)

SELECT *
FROM renamed
