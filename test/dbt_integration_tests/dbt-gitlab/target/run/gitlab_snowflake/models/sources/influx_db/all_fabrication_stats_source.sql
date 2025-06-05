
  
    

create or replace transient table EMBUCKET.influx_db.all_fabrication_stats_source
    

    
    as (WITH source AS (
  
   SELECT *
   FROM EMBUCKET.influx_db.all_fabrication_stats
 
), final AS (
 
    SELECT   
      timestamp::TIMESTAMP                  AS timestamp,
      resource::VARCHAR                     AS resource,
      fabrication_method::VARCHAR           AS fabrication_method,
      http_method::VARCHAR                  AS http_method,
      run_type::VARCHAR                     AS run_type,
      merge_request::VARCHAR                AS merge_request,
      fabrication_time::FLOAT               AS fabrication_time,
      info::VARCHAR                         AS info,
      job_url::VARCHAR                      AS job_url,
      _uploaded_at::TIMESTAMP               AS _uploaded_at,
      md5(cast(coalesce(cast(timestamp as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(resource as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fabrication_method as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(http_method as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(run_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(merge_request as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(fabrication_time as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(info as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(job_url as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(_uploaded_at as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS combined_composite_keys
    FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY _uploaded_at DESC) = 1
    )
;


  