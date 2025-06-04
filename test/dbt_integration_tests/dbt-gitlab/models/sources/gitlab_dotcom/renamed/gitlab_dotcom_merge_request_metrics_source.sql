WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_metrics_dedupe_source') }}
  

), renamed AS (

    SELECT

      id::NUMBER                                              AS merge_request_metric_id,
      merge_request_id::NUMBER                                AS merge_request_id,

      latest_build_started_at::TIMESTAMP                       AS latest_build_started_at,
      latest_build_finished_at::TIMESTAMP                      AS latest_build_finished_at,
      first_deployed_to_production_at::TIMESTAMP               AS first_deployed_to_production_at,
      merged_at::TIMESTAMP                                     AS merged_at,
      created_at::TIMESTAMP                                    AS created_at,
      updated_at::TIMESTAMP                                    AS updated_at,
      latest_closed_at::TIMESTAMP                              AS latest_closed_at,
      first_comment_at::TIMESTAMP                              AS first_comment_at,
      first_commit_at::TIMESTAMP                               AS first_commit_at,
      last_commit_at::TIMESTAMP                                AS last_commit_at,
      first_approved_at::TIMESTAMP                             AS first_approved_at,
      first_reassigned_at::TIMESTAMP                           AS first_reassigned_at,

      pipeline_id::NUMBER                                     AS pipeline_id,
      merged_by_id::NUMBER                                    AS merged_by_id,
      latest_closed_by_id::NUMBER                             AS latest_closed_by_id,
      diff_size::NUMBER                                       AS diff_size,
      modified_paths_size::NUMBER                             AS modified_paths_size,
      commits_count::NUMBER                                   AS commits_count,
      added_lines::NUMBER                                     AS added_lines,
      removed_lines::NUMBER                                   AS removed_lines

    FROM source

)

SELECT *
FROM renamed
