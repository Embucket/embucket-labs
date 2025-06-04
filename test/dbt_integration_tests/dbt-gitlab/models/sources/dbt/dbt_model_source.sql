{{ config({
    "unique_key": "run_unique_key"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'manifest') }}
    {% if is_incremental() %}
    WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}

), nodes AS (

    SELECT 
      d.value                                               AS data_by_row,
      jsontext['metadata']['dbt_version']::VARCHAR          AS dbt_version,
      jsontext['metadata']['dbt_schema_version']::VARCHAR   AS schema_version,
      jsontext['metadata']['generated_at']::TIMESTAMP       AS generated_at,
      uploaded_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['nodes']), outer => true) d

), parsed AS (

    SELECT
      data_by_row['unique_id']::VARCHAR                               AS unique_id,  
      data_by_row['name']::VARCHAR                                    AS name,
      data_by_row['alias']::VARCHAR                                   AS alias,
      data_by_row['database']::VARCHAR                                AS database_name,
      data_by_row['schema']::VARCHAR                                  AS schema_name,
      data_by_row['package_name']::VARCHAR                            AS package_name,
      data_by_row['tags']::ARRAY                                      AS tags,
      data_by_row['refs']::ARRAY                                      AS references,
      {{ dbt_utils.generate_surrogate_key(['unique_id', 'generated_at']) }}    AS run_unique_key,
      dbt_version,
      schema_version,
      generated_at,
      uploaded_at
    FROM nodes
    WHERE data_by_row['resource_type']::VARCHAR = 'model'

)

SELECT *
FROM parsed