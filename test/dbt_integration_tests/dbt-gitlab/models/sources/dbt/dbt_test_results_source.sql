{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'test') }}

), flattened AS (

    SELECT 
      d.value AS data_by_row,
      jsontext['metadata']['dbt_version']::VARCHAR                                             AS dbt_version,
      jsontext['metadata']['dbt_schema_version']::VARCHAR                                      AS schema_version,
      COALESCE(jsontext['metadata']['generated_at'], jsontext['generated_at'])::TIMESTAMP_NTZ  AS generated_at,
      uploaded_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['results']), outer => true) d

), v1model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT                                 AS test_execution_time_elapsed,
      data_by_row['unique_id']::VARCHAR                                    AS test_unique_id,
      data_by_row['status']::VARCHAR                                       AS status,
      data_by_row['message']::VARCHAR                                      AS message,
      dbt_version,
      schema_version,
      generated_at,
      {{ dbt_utils.generate_surrogate_key(['test_unique_id', 'generated_at']) }}    AS test_unique_key,
      uploaded_at
    FROM flattened
    WHERE dbt_version IS NOT NULL
      AND test_execution_time_elapsed IS NOT NULL
  
), v0model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT                                 AS test_execution_time_elapsed,
      data_by_row['node']['unique_id']::VARCHAR                            AS test_unique_id,
      CASE
        WHEN data_by_row['fail'] = 'true' THEN 'fail'
        WHEN data_by_row['warn'] = 'true' THEN 'warn'
        WHEN data_by_row['error']::VARCHAR IS NOT NULL THEN 'error'
        ELSE 'pass'
      END                                                                  AS status,
      data_by_row['error']::VARCHAR                                        AS message,
      'PRE 0.19.0'                                                         AS dbt_version,
      'https://schemas.getdbt.com/dbt/run-results/v0.json'                 AS schema_version,
      generated_at,
      {{ dbt_utils.generate_surrogate_key(['test_unique_id', 'generated_at']) }}    AS test_unique_key,
      uploaded_at
    FROM flattened
    WHERE dbt_version IS NULL

)

SELECT *
FROM v0model_parsed_out

UNION 

SELECT *
FROM v1model_parsed_out
