WITH source AS (

  SELECT * FROM {{ source('thanos', 'periodic_queries') }}
  WHERE source_file_name LIKE '%gitaly-rps.json%'

)

SELECT
  jsonpath_result.value['metric']['env']::VARCHAR                      AS metric_env,
  jsonpath_result.value['metric']['environment']::VARCHAR              AS metric_environment,
  jsonpath_result.value['metric']['monitor']::VARCHAR                  AS metric_monitor,
  jsonpath_result.value['metric']['ruler_cluster']::VARCHAR            AS metric_ruler_cluster,
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
  LATERAL FLATTEN(input => source.jsontext['gitaly_rate_5m']) AS jsonpath_root INNER JOIN
  LATERAL FLATTEN(input => source.jsontext['gitaly_rate_5m']['body']['data']['result'], outer => TRUE) AS jsonpath_result
WHERE result_type IS NOT NULL AND status_type IS NOT NULL
