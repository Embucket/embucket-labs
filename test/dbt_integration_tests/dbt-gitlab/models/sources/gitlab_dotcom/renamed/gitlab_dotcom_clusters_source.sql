    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_clusters_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                AS cluster_id,
      user_id::NUMBER           AS user_id,
      provider_type::NUMBER     AS provider_type_id,
      platform_type::NUMBER     AS platform_type_id,
      created_at::TIMESTAMP      AS created_at,
      updated_at::TIMESTAMP      AS updated_at,
      enabled::BOOLEAN           AS is_enabled,
      environment_scope::VARCHAR AS environment_scope,
      cluster_type::NUMBER      AS cluster_type_id,
      domain::VARCHAR            AS domain,
      managed::VARCHAR           AS managed

    FROM source

)


SELECT *
FROM renamed
