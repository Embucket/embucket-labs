WITH source_status AS (
    
    SELECT *
    FROM {{ ref('dbt_source_freshness') }}

), filtered_to_snapshots AS (

    SELECT DISTINCT
      table_name, 
      DATE_TRUNC('d', latest_load_at) AS successful_load_at 
    FROM source_status
    WHERE LOWER(table_name) LIKE '%snapshot%'
    ORDER BY 2 DESC

)

SELECT *
FROM filtered_to_snapshots

