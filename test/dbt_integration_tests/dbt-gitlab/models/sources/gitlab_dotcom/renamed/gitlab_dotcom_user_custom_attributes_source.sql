WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_user_custom_attributes_dedupe_source') }}

), renamed AS (

  SELECT
    user_id::NUMBER       AS user_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    key::VARCHAR          AS user_custom_key,
    value::VARCHAR        AS user_custom_value
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1


)

SELECT *
FROM renamed
