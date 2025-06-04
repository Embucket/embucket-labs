WITH source AS (

  SELECT *

  FROM {{ ref('gitlab_dotcom_catalog_resources_dedupe_source') }}

),

renamed AS (

  SELECT
    id::INT                                       AS id,
    project_id::INT                               AS project_id,
    created_at::TIMESTAMP                         AS created_at,
    state::INT                                    AS state,
    latest_released_at::TIMESTAMP                 AS latest_released_at,
    name::VARCHAR                                 AS name,
    description::VARCHAR                          AS description,
    visibility_level::INT                         AS visibility_level,
    search_vector::VARCHAR                        AS search_vector,
    verification_level::INT                       AS verification_level,
    last_30_day_usage_count::INT                  AS last_30_day_usage_count,
    last_30_day_usage_count_updated_at::TIMESTAMP AS last_30_day_usage_count_updated_at
  FROM source

)


SELECT *
FROM renamed
