WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_security_scans_dedupe_source') }}

),

renamed AS (

  SELECT
    id::NUMBER AS security_scan_id,
    created_at::TIMESTAMP AS created_at,
    updated_at::TIMESTAMP AS updated_at,
    build_id::NUMBER AS build_id,
    scan_type::NUMBER AS scan_type,
    project_id::NUMBER AS project_id,
    pipeline_id::NUMBER AS pipeline_id,
    latest::BOOLEAN AS is_latest,
    status::NUMBER AS security_scan_status
  FROM source

)


SELECT *
FROM renamed
