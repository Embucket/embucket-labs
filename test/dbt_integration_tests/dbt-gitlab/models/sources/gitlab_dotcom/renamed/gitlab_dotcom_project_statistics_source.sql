WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_statistics_dedupe_source') }}
  
), renamed AS (

  SELECT
    id::NUMBER                                     AS project_statistics_id,
    project_id::NUMBER                             AS project_id,
    namespace_id::NUMBER                           AS namespace_id,
    commit_count::NUMBER                           AS commit_count,
    storage_size::NUMBER                           AS storage_size,
    repository_size::NUMBER                        AS repository_size,
    container_registry_size::NUMBER                AS container_registry_size,
    lfs_objects_size::NUMBER                       AS lfs_objects_size,
    build_artifacts_size::NUMBER                   AS build_artifacts_size,
    packages_size::NUMBER                          AS packages_size,
    wiki_size::NUMBER                              AS wiki_size,
    shared_runners_seconds::NUMBER                 AS shared_runners_seconds,
    shared_runners_seconds_last_reset::TIMESTAMP    AS last_update_started_at
  FROM source

)

SELECT *
FROM renamed
