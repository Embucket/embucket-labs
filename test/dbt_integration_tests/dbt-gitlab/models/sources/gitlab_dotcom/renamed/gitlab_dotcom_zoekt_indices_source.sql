
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_zoekt_indices_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER                                AS id, 
    zoekt_enabled_namespace_id::NUMBER        AS zoekt_enabled_namespace_id, 
    zoekt_node_id::NUMBER                     AS zoekt_node_id,
    namespace_id::NUMBER                      AS namespace_id,
    state::NUMBER                             AS state,
    created_at::TIMESTAMP                     AS created_at,
    updated_at::TIMESTAMP                     AS updated_at 

  FROM source 
)

SELECT *
FROM renamed