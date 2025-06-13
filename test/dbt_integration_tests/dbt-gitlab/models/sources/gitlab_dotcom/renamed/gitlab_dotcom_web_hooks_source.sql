WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_web_hooks_dedupe_source') }}

), renamed AS (

  SELECT
    id::NUMBER                      AS web_hook_id,
    project_id::NUMBER              AS project_id,
    integration_id::NUMBER          AS integration_id,
    type::VARCHAR                   AS type,
    created_at::TIMESTAMP           AS created_at,
    updated_at::TIMESTAMP           AS updated_at

  FROM source

)

SELECT *
FROM renamed
