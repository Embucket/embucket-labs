WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_approvals_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                     AS approval_id,
    merge_request_id::NUMBER       AS merge_request_id,
    user_id::NUMBER                AS user_id,
    created_at::TIMESTAMP           AS created_at,
    updated_at::TIMESTAMP           AS updated_at

  FROM source

)

SELECT *
FROM renamed
