WITH source AS (

  SELECT *
  FROM {{ ref('thanos_stage_group_error_budget_indicators_source') }}
  WHERE is_success = TRUE
)

SELECT *
FROM source
