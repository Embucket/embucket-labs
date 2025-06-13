{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'run') }}

), flattened AS (

    SELECT 
      d.value AS data_by_row,
      jsontext['metadata']['dbt_version']::VARCHAR                                              AS dbt_version,
      jsontext['metadata']['dbt_schema_version']::VARCHAR                                       AS schema_version,
      COALESCE(jsontext['metadata']['generated_at'], jsontext['generated_at'])::TIMESTAMP_NTZ   AS generated_at,
      uploaded_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['results']), outer => true) d

), v1model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT            AS model_execution_time,
      data_by_row['unique_id']::VARCHAR               AS model_unique_id,
      IFNULL(data_by_row['status']::VARCHAR, False)   AS status,
      IFNULL(data_by_row['message']::VARCHAR, False)  AS message,
      timing.value['started_at']::TIMESTAMP           AS compilation_started_at,
      timing.value['completed_at']::TIMESTAMP         AS compilation_completed_at,
      uploaded_at,                                    -- uploaded_at
      dbt_version,
      schema_version,
      generated_at,
      {{ dbt_utils.generate_surrogate_key([
          'model_unique_id', 
          'compilation_started_at',
          'uploaded_at'
          ]) }}                                       AS run_unique_key
    FROM flattened
    LEFT JOIN LATERAL FLATTEN(INPUT => data_by_row['timing']::ARRAY, outer => true) timing
    WHERE dbt_version is not null
    AND IFNULL(timing.value['name'], 'compile') = 'compile'
  
), v0model_parsed_out AS (
  
    SELECT
      data_by_row['execution_time']::FLOAT                           AS model_execution_time,
      data_by_row['node']['unique_id']::VARCHAR                      AS model_unique_id,
      CASE
        WHEN data_by_row['skip']::BOOLEAN = TRUE THEN 'skipped'
        WHEN data_by_row['error']::VARCHAR IS NOT NULL THEN 'error'
        ELSE 'success'
      END                                                            AS status,
      IFNULL(data_by_row['error']::VARCHAR, 'SUCCESS 1')             AS message,
      timing.value['started_at']::TIMESTAMP                          AS compilation_started_at,
      timing.value['completed_at']::TIMESTAMP                        AS compilation_completed_at,
      uploaded_at,                                                   -- uploaded_at
      'PRE 0.19.0'                                                   AS dbt_version,
      'https://schemas.getdbt.com/dbt/run-results/v0.json'           AS schema_version,
      generated_at,
      {{ dbt_utils.generate_surrogate_key([
          'model_unique_id', 
          'compilation_started_at',
          'uploaded_at'
          ]) }}                                                      AS run_unique_key
    FROM flattened
    LEFT JOIN LATERAL FLATTEN(INPUT => data_by_row['timing']::ARRAY, outer => true) timing
    WHERE dbt_version is null
    AND IFNULL(timing.value['name'], 'compile') = 'compile'
)

SELECT *
FROM v0model_parsed_out

UNION

SELECT *
FROM v1model_parsed_out