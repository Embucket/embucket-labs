WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_banned_users_dedupe_source') }}
  
), renamed AS (

  SELECT
    user_id::NUMBER                       AS user_id,
    created_at::TIMESTAMP                 AS created_at,
    updated_at::TIMESTAMP                 AS updated_at,
    pgp_is_deleted::BOOLEAN               AS is_deleted,
    pgp_is_deleted_updated_at::TIMESTAMP  AS is_deleted_updated_at

  FROM source

)

SELECT *
FROM renamed
