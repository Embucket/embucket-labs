{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'freshness') }}

), v0parsed AS (

    SELECT 
      REGEXP_REPLACE(s.path, '\\[|\\]|''', '')::VARCHAR                                           AS source_unique_id,
      REPLACE(REGEXP_REPLACE(s.path, '\\[|\\]|''', ''), 'source.gitlab_snowflake.', '')::VARCHAR  AS schema_table_name,
      SPLIT_PART(schema_table_name, '.', 1)                                                       AS schema_name,
      SPLIT_PART(schema_table_name, '.', -1)                                                      AS table_name,
      s.value['max_loaded_at']::TIMESTAMP                                                         AS latest_load_at,
      s.value['max_loaded_at_time_ago_in_s']::FLOAT                                               AS time_since_loaded_seconds,
      s.value['state']::VARCHAR                                                                   AS source_freshness_state,
      s.value['snapshotted_at']::TIMESTAMP                                                        AS freshness_observed_at,
      {{ dbt_utils.generate_surrogate_key(['schema_table_name', 'freshness_observed_at']) }}               AS freshness_unique_key,
      'PRE 0.19.0'                                                                                AS dbt_version,
      'https://schemas.getdbt.com/dbt/sources/v0.json'                                            AS schema_version,
      uploaded_at
    FROM source 
    INNER JOIN LATERAL FLATTEN(jsontext['sources']) s
    WHERE jsontext['metadata']['dbt_version'] IS NULL
		AND s.value['state']::VARCHAR != 'runtime error'                                          -- impossible to know what freshness is, so filtered out
        AND s.value['max_loaded_at']::TIMESTAMP    IS NOT NULL                                    -- latest_load_at
        AND s.value['snapshotted_at']::TIMESTAMP   IS NOT NULL                                    -- freshness_observed_at
        AND s.value['max_loaded_at_time_ago_in_s']::FLOAT IS NOT NULL                             -- time_since_loaded_seconds

), v1parsed AS (

    SELECT 
      s.value['unique_id']::VARCHAR                                                               AS source_unique_id,
      REPLACE(s.value['unique_id'], 'source.gitlab_snowflake.', '')::VARCHAR                      AS schema_table_name,
      SPLIT_PART(schema_table_name, '.', 1)                                                       AS schema_name,
      SPLIT_PART(schema_table_name, '.', -1)                                                      AS table_name,
      s.value['max_loaded_at']::TIMESTAMP                                                         AS latest_load_at,
      s.value['max_loaded_at_time_ago_in_s']::FLOAT                                               AS time_since_loaded_seconds,
      s.value['status']::VARCHAR                                                                  AS source_freshness_state,
      s.value['snapshotted_at']::TIMESTAMP                                                        AS freshness_observed_at,
      {{ dbt_utils.generate_surrogate_key(['schema_table_name', 'freshness_observed_at']) }}               AS freshness_unique_key,
      jsontext['metadata']['dbt_version']::VARCHAR                                                AS dbt_version,
      jsontext['metadata']['dbt_schema_version']::VARCHAR                                         AS schema_version,
      uploaded_at
    FROM source 
    INNER JOIN LATERAL FLATTEN(jsontext['results']) s
    WHERE jsontext['metadata']['dbt_version'] IS NOT NULL
    AND s.value['max_loaded_at']::TIMESTAMP    IS NOT NULL                                        -- latest_load_at
    AND s.value['snapshotted_at']::TIMESTAMP   IS NOT NULL                                        -- freshness_observed_at
    AND s.value['max_loaded_at_time_ago_in_s']::FLOAT IS NOT NULL                                 -- time_since_loaded_seconds
)


SELECT *
FROM v1parsed

UNION 

SELECT *
FROM v0parsed