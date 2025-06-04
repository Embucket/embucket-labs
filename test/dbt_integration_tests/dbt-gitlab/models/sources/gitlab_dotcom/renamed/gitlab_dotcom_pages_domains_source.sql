    
WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_pages_domains_dedupe_source') }}
    
), renamed AS (
  
    SELECT
    
      id::NUMBER                              AS pages_domain_id,
      project_id::NUMBER                      AS project_id,
      verified_at::TIMESTAMP                  AS verified_at,
      enabled_until::TIMESTAMP                AS enabled_until,
      remove_at::TIMESTAMP                    AS remove_at,
      auto_ssl_enabled::BOOLEAN               AS is_auto_ssl_enabled,
      wildcard::BOOLEAN                       AS wildcard,
      usage::NUMBER                           AS usage,
      scope::NUMBER                           AS scope,
      auto_ssl_failed:BOOLEAN                 AS is_auto_ssl_failed
    
    FROM source
      
)

SELECT * 
FROM renamed
