WITH source AS (
  
    SELECT * 
    FROM {{ source('engineering', 'lcp') }}
    
), metric_per_row AS (

    SELECT 
      data_by_row.value['datapoints']::ARRAY                       AS datapoints,
      data_by_row.value['target']::VARCHAR                         AS metric_name,
      IFF(SPLIT_PART(metric_name, '.', 3) = 'gitlab', 1, 0)        AS version_offset,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(jsontext), OUTER => True) data_by_row

), data_points_flushed_out AS (

    SELECT
      SPLIT_PART(metric_name, '.', 13 + version_offset)::VARCHAR  AS aggregation_name,
      SPLIT_PART(metric_name, '.', 5 + version_offset)::VARCHAR   AS metric_name,        
      data_by_row.value[0]::FLOAT                                 AS metric_value,
      data_by_row.value[1]::TIMESTAMP                             AS metric_reported_at
    FROM metric_per_row,
    LATERAL FLATTEN(input => datapoints, OUTER => True) data_by_row
    WHERE NULLIF(metric_value::VARCHAR, 'null') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY metric_name, aggregation_name, metric_reported_at ORDER BY uploaded_at DESC) = 1

)

SELECT *
FROM data_points_flushed_out
ORDER BY metric_reported_at