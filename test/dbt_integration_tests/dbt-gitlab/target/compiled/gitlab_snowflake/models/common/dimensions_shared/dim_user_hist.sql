

WITH dim_user_snapshot AS (

    SELECT
      md5(cast(coalesce(cast(dbt_updated_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(dim_user_sk as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS dim_user_snapshot_hist_id,
	  *
    FROM "EMBUCKET".snapshots.dim_user_snapshot

)

SELECT 
  *
FROM
  dim_user_snapshot