{{ config({
    "alias": "gitlab_dotcom_namespace_root_storage_statistics_snapshots"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('snapshots', 'gitlab_dotcom_namespace_root_storage_statistics_snapshots') }}
    
), renamed as (

    SELECT
        dbt_scd_id::VARCHAR                                           AS namespace_storage_statistics_snapshot_id,
        namespace_id::NUMBER                                         AS namespace_id,
        repository_size::NUMBER                                      AS repository_size,
        lfs_objects_size::NUMBER                                     AS lfs_objects_size,
        wiki_size::NUMBER                                            AS wiki_size,
        build_artifacts_size::NUMBER                                 AS build_artifacts_size,
        storage_size::NUMBER                                         AS storage_size,
        packages_size::NUMBER                                        AS packages_size,
        container_registry_size::NUMBER                              AS container_registry_size,
        dbt_valid_from::TIMESTAMP                                     AS valid_from,
        dbt_valid_to::TIMESTAMP                                       AS valid_to

    FROM source
    
)

SELECT *
FROM renamed
