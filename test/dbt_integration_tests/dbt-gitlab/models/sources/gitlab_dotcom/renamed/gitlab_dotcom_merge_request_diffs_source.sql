WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diffs_dedupe_source') }}
  WHERE created_at IS NOT NULL
    AND updated_at IS NOT NULL

),

renamed AS (

  SELECT
    id        AS merge_request_diff_id,
    base_commit_sha,
    head_commit_sha,
    start_commit_sha,
    state     AS merge_request_diff_status,
    merge_request_id,
    real_size AS merge_request_real_size,
    commits_count,
    created_at,
    updated_at,
    external_diff,
    external_diff_store,
    stored_externally,
    files_count,
    sorted,
    diff_type,
    _uploaded_at
  FROM source

)

SELECT *
FROM renamed
