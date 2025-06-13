{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('version', 'conversational_development_indices') }}
    {% if is_incremental() %}
    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), renamed AS (

    SELECT
      id::NUMBER                                   AS id,
      usage_data_id::NUMBER                        AS usage_data_id,
      leader_boards::FLOAT                         AS leader_boards,
      instance_boards::FLOAT                       AS instance_boards,
      leader_ci_pipelines::FLOAT                   AS leader_ci_pipelines,
      instance_ci_pipelines::FLOAT                 AS instance_ci_pipelines,
      leader_deployments::FLOAT                    AS leader_deployments,
      instance_deployments::FLOAT                  AS instance_deployments,
      leader_environments::FLOAT                   AS leader_environments,
      instance_environments::FLOAT                 AS instance_environments,
      leader_issues::FLOAT                         AS leader_issues,
      instance_issues::FLOAT                       AS instance_issues,
      leader_merge_requests::FLOAT                 AS leader_merge_requests,
      instance_merge_requests::FLOAT               AS instance_merge_requests,
      leader_milestones::FLOAT                     AS leader_milestones,
      instance_milestones::FLOAT                   AS instance_milestones,
      leader_notes::FLOAT                          AS leader_notes,
      instance_notes::FLOAT                        AS instance_notes,
      leader_projects_prometheus_active::FLOAT     AS leader_projects_prometheus_active,
      instance_projects_prometheus_active::FLOAT   AS instance_projects_prometheus_active,
      leader_service_desk_issues::FLOAT            AS leader_service_desk_issues,
      instance_service_desk_issues::FLOAT          AS instance_service_desk_issues,
      percentage_boards::FLOAT                     AS percentage_boards,
      percentage_ci_pipelines::FLOAT               AS percentage_ci_pipelines,
      percentage_deployments::FLOAT                AS percentage_deployments,
      percentage_environments::FLOAT               AS percentage_environments,
      percentage_issues::FLOAT                     AS percentage_issues,
      percentage_merge_requests::FLOAT             AS percentage_merge_requests,
      percentage_milestones::FLOAT                 AS percentage_milestones,
      percentage_notes::FLOAT                      AS percentage_notes,
      percentage_projects_prometheus_active::FLOAT AS percentage_projects_prometheus_active,
      percentage_service_desk_issues::FLOAT        AS percentage_service_desk_issues,
      created_at::TIMESTAMP                        AS created_at,
      updated_at::TIMESTAMP                        AS updated_at
    FROM source

)

SELECT *
FROM renamed
