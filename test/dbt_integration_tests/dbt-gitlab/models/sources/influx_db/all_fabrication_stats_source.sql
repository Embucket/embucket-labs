WITH source AS (
  
   SELECT *
   FROM {{ source('influx_db','all_fabrication_stats') }}
 
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
      {{ dbt_utils.generate_surrogate_key(['timestamp', 'resource', 'fabrication_method', 'http_method', 'run_type', 'merge_request', 'fabrication_time', 'info', 'job_url', '_uploaded_at']) }} AS combined_composite_keys
    FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY _uploaded_at DESC) = 1
