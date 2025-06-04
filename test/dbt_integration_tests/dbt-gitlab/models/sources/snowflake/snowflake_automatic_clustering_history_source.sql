WITH source AS (

  SELECT *
  FROM {{ source('snowflake_account_usage', 'automatic_clustering_history') }}

),

renamed AS (
SELECT
    start_time::TIMESTAMP         AS clustering_start_at,
    end_time::TIMESTAMP           AS clustering_end_at,
    credits_used::NUMBER          AS credits_used,
    num_bytes_reclustered::NUMBER AS bytes_reclustered,
    num_rows_reclustered::NUMBER  AS rows_reclustered,
    table_id::NUMBER              AS table_id,
    table_name::VARCHAR           AS table_name,
    schema_id::NUMBER             AS schema_id,
    schema_name::VARCHAR          AS schema_name,
    database_id::NUMBER           AS database_id,
    database_name::VARCHAR        AS database_name,
    instance_id::NUMBER           AS instance_id,
    LOWER(database_name || '.' || schema_name || '.' || table_name) AS full_table_name
FROM source

)

SELECT *
FROM renamed
