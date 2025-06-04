WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_root_storage_statistics_dedupe_source') }}
  
), renamed AS (

    SELECT

      namespace_id::NUMBER                 AS namespace_id,
      repository_size::NUMBER              AS repository_size,
      container_registry_size::NUMBER      AS container_registry_size,
      lfs_objects_size::NUMBER             AS lfs_objects_size,
      wiki_size::NUMBER                    AS wiki_size,
      build_artifacts_size::NUMBER         AS build_artifacts_size,
      storage_size::NUMBER                 AS storage_size,
      packages_size::NUMBER                AS packages_size,
      updated_at::TIMESTAMP                AS namespace_updated_at

    FROM source

)

SELECT *
FROM renamed
