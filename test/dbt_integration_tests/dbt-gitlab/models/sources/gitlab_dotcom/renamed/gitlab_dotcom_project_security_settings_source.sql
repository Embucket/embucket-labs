WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_security_settings_dedupe_source') }}
  
), renamed AS (

  SELECT
    project_id::NUMBER                                  AS project_id,
    created_at::TIMESTAMP                               AS created_at,
    updated_at::TIMESTAMP                               AS updated_at,
    pre_receive_secret_detection_enabled::BOOLEAN       AS is_pre_receive_secret_detection_enabled,
    secret_push_protection_enabled::BOOLEAN             AS is_secret_push_protection_enabled
  FROM source

)


SELECT *
FROM renamed
