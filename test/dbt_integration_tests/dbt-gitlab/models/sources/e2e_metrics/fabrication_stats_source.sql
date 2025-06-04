WITH source AS (

  SELECT
    PARSE_JSON(payload) AS payload,
    uploaded_at
  FROM {{ source('e2e_metrics','fabrication_stats') }}

),

final AS (

  SELECT
    payload:fields.fabrication_time::NUMBER  AS fabrication_time,
    payload:fields.info::VARCHAR             AS info,
    payload:fields.job_url::VARCHAR          AS job_url,
    payload:fields.timestamp::VARCHAR        AS timestamp,
    payload:name::VARCHAR                    AS name,
    payload:time::TIMESTAMP                  AS time,
    payload:tags.fabrication_method::VARCHAR AS tags_fabrication_method,
    payload:tags.http_method::VARCHAR        AS tags_http_method,
    payload:tags.merge_request::BOOLEAN      AS is_merge_request,
    payload:tags.resource::VARCHAR           AS tags_resource,
    payload:tags.run_type::VARCHAR           AS tags_run_type,
    uploaded_at                              AS uploaded_at,
    {{ dbt_utils.generate_surrogate_key(['timestamp', 'tags_resource', 'tags_fabrication_method', 'tags_http_method', 'tags_run_type', 'is_merge_request', 'fabrication_time', 'info', 'job_url', 'uploaded_at']) }} AS combined_composite_keys
  FROM source
)

SELECT *
FROM final
QUALIFY ROW_NUMBER() OVER (PARTITION BY combined_composite_keys ORDER BY uploaded_at DESC) = 1
