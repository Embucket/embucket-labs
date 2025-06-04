WITH source AS (

  SELECT * FROM {{ source('thanos', 'periodic_queries') }}
  WHERE source_file_name LIKE '%daily-slas%'

),

overall_ratio AS (

  SELECT
    'overall_ratio'                                                      AS metric_query,
    jsonpath_result.value['metric']['env']::VARCHAR                      AS metric_env,
    jsonpath_result.value['metric']['environment']::VARCHAR              AS metric_environment,
    jsonpath_result.value['metric']['monitor']::VARCHAR                  AS metric_monitor,
    jsonpath_result.value['metric']['ruler_cluster']::VARCHAR            AS metric_ruler_cluster,
    jsonpath_result.value['metric']['sla_type']::VARCHAR                 AS metric_sla_type,
    jsonpath_result.value['metric']['stage']::VARCHAR                    AS metric_stage,
    PARSE_JSON(jsonpath_result.value:value[0]::FLOAT)::NUMBER::TIMESTAMP AS metric_created_at,
    jsonpath_result.value:value[1]::FLOAT                                AS metric_value,
    jsonpath_root.value['data']['resultType']::VARCHAR                   AS result_type,
    jsonpath_root.value['status']::VARCHAR                               AS status_type,
    jsonpath_root.this['message']::VARCHAR                               AS message_type,
    jsonpath_root.this['status_code']::NUMBER                            AS status_code,
    jsonpath_root.this['success']::BOOLEAN                               AS is_success
  FROM
    source INNER JOIN
    LATERAL FLATTEN(input => source.jsontext['overall_ratio']) jsonpath_root INNER JOIN
    LATERAL FLATTEN(input => source.jsontext['overall_ratio']['body']['data']['result'], outer => TRUE) jsonpath_result
  WHERE result_type IS NOT NULL AND status_type IS NOT NULL
),

overall_target AS (

  SELECT
    'overall_target'                                                     AS metric_query,
    NULL                                                                 AS metric_env,
    NULL                                                                 AS metric_environment,
    jsonpath_result.value['metric']['monitor']::VARCHAR                  AS metric_monitor,
    jsonpath_result.value['metric']['ruler_cluster']::VARCHAR            AS metric_ruler_cluster,
    NULL                                                                 AS metric_sla_type,
    NULL                                                                 AS metric_stage,
    PARSE_JSON(jsonpath_result.value:value[0]::FLOAT)::NUMBER::TIMESTAMP AS metric_created_at,
    jsonpath_result.value:value[1]::FLOAT                                AS metric_value,
    jsonpath_root.value['data']['resultType']::VARCHAR                   AS result_type,
    jsonpath_root.value['status']::VARCHAR                               AS status_type,
    jsonpath_root.this['message']::VARCHAR                               AS message_type,
    jsonpath_root.this['status_code']::NUMBER                            AS status_code,
    jsonpath_root.this['success']::BOOLEAN                               AS is_success
  FROM
    source INNER JOIN
    LATERAL FLATTEN(input => source.jsontext['overall_target']) jsonpath_root INNER JOIN
    LATERAL FLATTEN(input => source.jsontext['overall_target']['body']['data']['result'], outer => TRUE) jsonpath_result
  WHERE result_type IS NOT NULL 
    AND status_type IS NOT NULL
    AND metric_created_at IS NOT NULL

),

unioned AS (

  SELECT
    *,
    RANK() OVER (PARTITION BY DATE(metric_created_at)
      ORDER BY metric_created_at ASC NULLS LAST) AS nth_daily_measurement
  FROM overall_ratio

  UNION ALL

  SELECT
    *,
    RANK() OVER (PARTITION BY DATE(metric_created_at)
      ORDER BY metric_created_at ASC NULLS LAST) AS nth_daily_measurement
  FROM overall_target

)

SELECT * FROM unioned
WHERE nth_daily_measurement = 1
