
{{ config({
    "materialized": "incremental",
    "unique_key": "merge_request_id"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_ops', 'merge_requests') }}
  
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT

      id::NUMBER                                                AS merge_request_id,
      iid::NUMBER                                               AS merge_request_iid,
      title::VARCHAR                                            AS merge_request_title,

      IFF(lower(target_branch) = 'master', TRUE, FALSE)         AS is_merge_to_master,
      IFF(lower(merge_error) = 'nan', NULL, merge_error)        AS merge_error,
      assignee_id::NUMBER                                       AS assignee_id,
      updated_by_id::NUMBER                                     AS updated_by_id,
      merge_user_id::NUMBER                                     AS merge_user_id,
      last_edited_by_id::NUMBER                                 AS last_edited_by_id,
      milestone_id::NUMBER                                      AS milestone_id,
      head_pipeline_id::NUMBER                                  AS head_pipeline_id,
      latest_merge_request_diff_id::NUMBER                      AS latest_merge_request_diff_id,
      approvals_before_merge::NUMBER                            AS approvals_before_merge,
      lock_version::NUMBER                                      AS lock_version,
      time_estimate::NUMBER                                     AS time_estimate,
      source_project_id::NUMBER                                 AS project_id,
      target_project_id::NUMBER                                 AS target_project_id,
      author_id::NUMBER                                         AS author_id,
      state_id::NUMBER                                          AS merge_request_state_id,
      -- Override state by mapping state_id. See issue #3556.
      {{ map_state_id('state_id') }}                            AS merge_request_state,
      merge_status                                              AS merge_request_status,
      merge_when_pipeline_succeeds::BOOLEAN                     AS does_merge_when_pipeline_succeeds,
      squash::BOOLEAN                                           AS does_squash,
      discussion_locked::BOOLEAN                                AS is_discussion_locked,
      allow_maintainer_to_push::BOOLEAN                         AS does_allow_maintainer_to_push,
      created_at::TIMESTAMP                                     AS created_at,
      updated_at::TIMESTAMP                                     AS updated_at,
      last_edited_at::TIMESTAMP                                 AS merge_request_last_edited_at

      --merge_params // hidden for privacy

    FROM source

)

SELECT  *
FROM renamed
ORDER BY updated_at

