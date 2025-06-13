WITH base AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'merge_request_diff_commit_users') }}

)

{{ scd_latest_state() }}
