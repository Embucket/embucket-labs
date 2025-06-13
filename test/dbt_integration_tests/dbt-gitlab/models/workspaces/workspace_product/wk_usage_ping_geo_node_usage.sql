{{ config({
    "materialized": "incremental",
    "unique_key": "instance_path_id",
    "schema": "legacy"
    })
}}

WITH prep_usage_data_flattened AS (

    SELECT * FROM {{ ref('prep_ping_instance_flattened')}}
    {% if is_incremental() %}

      WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{this}})

    {% endif %}

)


, data AS (

    SELECT *
    FROM prep_usage_data_flattened
    WHERE metrics_path = 'usage_activity_by_stage_monthly.enablement.geo_node_usage'
        AND metric_value <> '[]'

), flattened AS (

SELECT *
FROM data,
lateral flatten(input => metric_value,
recursive => true)

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['flattened.dim_ping_instance_id', 'flattened.path']) }} AS instance_path_id,
  flattened.dim_instance_id AS instance_id,
  flattened.dim_ping_instance_id AS ping_id,
  flattened.original_edition AS edition,
  flattened.dim_host_id AS host_id,
  flattened.ping_created_at,
  flattened.uploaded_at,
  prep_ping_instance.version,
  REGEXP_REPLACE(NULLIF(prep_ping_instance.version, ''), '[^0-9.]+') AS cleaned_version,
  SPLIT_PART(cleaned_version, '.', 1)::NUMBER AS major_version,
  SPLIT_PART(cleaned_version, '.', 2)::NUMBER AS minor_version,
  major_version || '.' || minor_version AS major_minor_version,
  regexp_replace(split_part(path, '.', 1), '(\\[|\\])', '') AS node_id,
  value AS metric_value,
  SPLIT_PART(path, '.', -1) AS metrics_path
FROM flattened
LEFT JOIN {{ ref('prep_ping_instance')}}
  ON flattened.dim_ping_instance_id = prep_ping_instance.dim_ping_instance_id
WHERE index IS NULL
