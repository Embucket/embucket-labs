WITH dbt_source AS (

    SELECT *
    FROM {{ ref('dbt_source_freshness_results_source') }}
    WHERE latest_load_at IS NOT NULL

)

SELECT * 
FROM dbt_source
