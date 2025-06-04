

WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_merge_request_diff_commit_users_source') }}

)

SELECT *
FROM source
