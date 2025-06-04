
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_zoekt_enabled_namespaces_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER                                AS id,
    root_namespace_id::NUMBER                 AS root_namespace_id,
    search::BOOLEAN                           AS search,
    created_at::TIMESTAMP                     AS created_at,
    updated_at::TIMESTAMP                     AS updated_at 
  FROM source 
)

SELECT *
FROM renamed