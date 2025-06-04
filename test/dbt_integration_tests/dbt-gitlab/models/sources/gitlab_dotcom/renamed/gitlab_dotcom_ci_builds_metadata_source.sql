    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_builds_metadata_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                          AS ci_builds_metadata_id,
    project_id::NUMBER                  AS project_id,
    timeout::TIMESTAMP                  AS timeout,
    timeout_source::VARCHAR             AS timeout_source,
    interruptible::NUMBER               AS interruptible,
    has_exposed_artifacts::BOOLEAN      AS has_exposed_artifacts,
    exit_code::NUMBER                   AS exit_code,
    build_id::NUMBER                    AS build_id

  FROM source

)


SELECT *
FROM renamed
