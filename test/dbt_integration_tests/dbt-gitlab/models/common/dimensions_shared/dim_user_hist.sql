{{ config({
        "materialized": "table",
        "unique_key": "dim_user_snapshot_hist_id",
        "tags": ["edm_snapshot", "user_snapshots"]
    })
}}

WITH dim_user_snapshot AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['dbt_updated_at', 'dim_user_sk']) }} AS dim_user_snapshot_hist_id,
	  *
    FROM {{ source('snapshots','dim_user_snapshot') }}

)

SELECT 
  *
FROM
  dim_user_snapshot  
