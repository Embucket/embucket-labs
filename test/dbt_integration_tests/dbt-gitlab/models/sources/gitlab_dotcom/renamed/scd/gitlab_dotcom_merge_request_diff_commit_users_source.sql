WITH source AS (

  SELECT *

  FROM {{ ref('gitlab_dotcom_merge_request_diff_commit_users_dedupe_source') }}

),

renamed AS (

  SELECT
    id,
    name,
    email,
    _uploaded_at
  FROM source

)


SELECT *
FROM renamed
