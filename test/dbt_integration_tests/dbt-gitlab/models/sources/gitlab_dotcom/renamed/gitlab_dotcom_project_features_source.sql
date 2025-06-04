    
WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_project_features_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                     AS project_feature_id,
      project_id::NUMBER                             AS project_id,
      merge_requests_access_level::NUMBER            AS merge_requests_access_level,
      issues_access_level::NUMBER                    AS issues_access_level,
      wiki_access_level::NUMBER                      AS wiki_access_level,
      snippets_access_level::NUMBER                  AS snippets_access_level,
      builds_access_level::NUMBER                    AS builds_access_level,
      repository_access_level::NUMBER                AS repository_access_level,
      pages_access_level::NUMBER                     AS pages_access_level, 
      forking_access_level::NUMBER                   AS forking_access_level,
      metrics_dashboard_access_level::NUMBER         AS metrics_dashboard_access_level,
      requirements_access_level::NUMBER              AS requirements_access_level, 
      operations_access_level::NUMBER                AS operations_access_level,
      analytics_access_level::NUMBER                 AS analytics_access_level,
      security_and_compliance_access_level::NUMBER   AS security_and_compliance_access_level, 
      container_registry_access_level::NUMBER        AS container_registry_access_level, 
      package_registry_access_level::NUMBER          AS package_registry_access_level, 
      monitor_access_level::NUMBER                   AS monitor_access_level, 
      infrastructure_access_level::NUMBER            AS infrastructure_access_level, 
      feature_flags_access_level::NUMBER             AS feature_flags_access_level, 
      environments_access_level::NUMBER              AS environments_access_level, 
      releases_access_level::NUMBER                  AS releases_access_level,
      created_at::TIMESTAMP                          AS created_at,
      updated_at::TIMESTAMP                          AS updated_at

    FROM source

)

SELECT *
FROM renamed
