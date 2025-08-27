{{ config({
    "alias": "gitlab_dotcom_project_statistics_snapshot"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_project_statistics_snapshots') }}

), renamed AS (

    SELECT
      dbt_scd_id::VARCHAR                             AS project_statistics_snapshot_id,
      id::NUMBER                                      AS project_statistics_id,
      project_id::NUMBER                              AS project_id,
      namespace_id::NUMBER                            AS namespace_id,
      commit_count::NUMBER                            AS commit_count,
      storage_size::NUMBER                            AS storage_size,
      repository_size::NUMBER                         AS repository_size,
      container_registry_size::NUMBER                 AS container_registry_size,
      lfs_objects_size::NUMBER                        AS lfs_objects_size,
      build_artifacts_size::NUMBER                    AS build_artifacts_size,
      packages_size::NUMBER                           AS packages_size,
      wiki_size::NUMBER                               AS wiki_size,
      shared_runners_seconds::NUMBER                  AS shared_runners_seconds,
      shared_runners_seconds_last_reset::TIMESTAMP    AS last_update_started_at,
      DATEADD('sec', _uploaded_at, '1970-01-01')      AS uploaded_at,
      dbt_valid_from::TIMESTAMP                     AS valid_from,
      dbt_valid_to::TIMESTAMP                       AS valid_to

    FROM source

)

SELECT *
FROM renamed
