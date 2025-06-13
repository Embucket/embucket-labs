WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_vulnerabilities_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                        AS vulnerability_id,
      confidence::NUMBER                AS confidence,
      confidence_overridden::BOOLEAN    AS is_confidence_overridden, 
      confirmed_at::TIMESTAMP           AS confirmed_at,
      created_at::TIMESTAMP             AS created_at,
      dismissed_at::TIMESTAMP           AS dismissed_at,
      resolved_at::TIMESTAMP            AS resolved_at,
      severity_overridden::BOOLEAN      AS is_severity_overriden,
      state::NUMBER                     AS state,
      updated_at::TIMESTAMP             AS updated_at
    FROM source
    
)

SELECT *
FROM renamed
