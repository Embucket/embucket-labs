WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_namespace_monthly_usages_dedupe_source') }}
      
)

, renamed AS (
    SELECT
    id::NUMBER                                  AS ci_namespace_monthly_usages_id,
    namespace_id::NUMBER                        AS namespace_id,
    date::TIMESTAMP                             AS date,
    amount_used::NUMBER                         AS amount_used,
    notification_level::NUMBER                  AS notification_level,
    shared_runners_duration::NUMBER             AS shared_runners_duration,
    created_at::TIMESTAMP                       AS created_at
    FROM source
  
)

SELECT * 
FROM renamed
