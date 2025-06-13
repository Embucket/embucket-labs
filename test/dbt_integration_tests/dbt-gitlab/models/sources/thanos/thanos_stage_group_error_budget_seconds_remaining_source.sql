WITH source AS (

  SELECT *
  FROM {{ source('thanos', 'periodic_queries') }}

),

parsed AS (

  SELECT
    pq_2.value['metric']['product_stage']::VARCHAR            AS metric_product_stage,
    pq_2.value['metric']['stage_group']::VARCHAR              AS metric_stage_group,
    PARSE_JSON(pq_2.value:value[0]::FLOAT)::NUMBER::TIMESTAMP AS metric_created_at,
    NULLIF(pq_2.value:value[1], 'NaN')::FLOAT                 AS metric_value,
    pq_1.value['data']['resultType']::VARCHAR                 AS result_type,
    pq_1.value['status']::VARCHAR                             AS status_type,
    pq_1.this['message']::VARCHAR                             AS message_type,
    pq_1.this['status_code']::NUMBER                          AS status_code,
    pq_1.this['success']::BOOLEAN                             AS is_success,
    SPLIT_PART(pq_0.source_file_name, '/', -1)                AS source_file_name
  FROM
    source pq_0,
    LATERAL FLATTEN(input => pq_0.jsontext['stage_group_error_budget_seconds_remaining']) pq_1,
    LATERAL FLATTEN(input => pq_0.jsontext['stage_group_error_budget_seconds_remaining']['body']['data']['result'], outer => TRUE) pq_2
  WHERE result_type IS NOT NULL AND status_type IS NOT NULL

)

SELECT * FROM
  parsed
