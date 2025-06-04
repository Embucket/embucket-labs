WITH source AS (
  SELECT *
  FROM {{ source('thanos', 'periodic_queries') }}

),

parsed AS (

  SELECT
    'stage_group_error_budget_completeness'                     AS source_metric_name,
    PARSE_JSON(pq_2.value:value[0]::DOUBLE)::INT::TIMESTAMP_NTZ AS metric_created_at,
    NULLIF(pq_2.value:value[1], 'NaN')::DOUBLE                  AS metric_value,
    pq_1.value['data']['resultType']::VARCHAR                   AS result_type,
    pq_1.value['status']::VARCHAR                               AS status_type,
    pq_1.this['message']::VARCHAR                               AS message_type,
    pq_1.this['status_code']::VARCHAR                           AS status_code,
    pq_1.this['success']::VARCHAR                               AS is_success,
    SPLIT_PART(pq_0.source_file_name, '/', -1)                  AS source_file_name
  FROM
    source pq_0,
    LATERAL FLATTEN(input => pq_0.jsontext['stage_group_error_budget_completeness']) pq_1,
    LATERAL FLATTEN(input => pq_0.jsontext['stage_group_error_budget_completeness']['body']['data']['result'], outer => TRUE) pq_2
  WHERE result_type IS NOT NULL AND status_type IS NOT NULL
  UNION 
  SELECT
    'stage_group_error_budget_teams_over_budget_availability'   AS source_metric_name,
    PARSE_JSON(pq_2.value:value[0]::DOUBLE)::INT::TIMESTAMP_NTZ AS metric_created_at,
    NULLIF(pq_2.value:value[1], 'NaN')::DOUBLE                  AS metric_value,
    pq_1.value['data']['resultType']::VARCHAR                   AS result_type,
    pq_1.value['status']::VARCHAR                               AS status_type,
    pq_1.this['message']::VARCHAR                               AS message_type,
    pq_1.this['status_code']::VARCHAR                           AS status_code,
    pq_1.this['success']::VARCHAR                               AS is_success,
    SPLIT_PART(pq_0.source_file_name, '/', -1)                  AS source_file_name
  FROM
    source pq_0,
    LATERAL FLATTEN(input => pq_0.jsontext['stage_group_error_budget_teams_over_budget_availability']) pq_1,
    LATERAL FLATTEN(input => pq_0.jsontext['stage_group_error_budget_teams_over_budget_availability']['body']['data']['result'], outer => TRUE) pq_2
  WHERE result_type IS NOT NULL AND status_type IS NOT NULL
)

SELECT * FROM
  parsed
