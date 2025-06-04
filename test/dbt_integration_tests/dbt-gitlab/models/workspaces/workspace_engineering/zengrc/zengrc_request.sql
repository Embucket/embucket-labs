WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_request_source') }}

)

SELECT
  request_id, 
  request_code,
  request_created_at,
  request_custom_attributes,
  request_description,
  request_end_date,
  request_start_date,
  request_status,
  request_stop_date,
  request_tags,
  request_title,
  audit_id,
  audit_title,
  zengrc_object_type,
  request_updated_at,
  request_loaded_at,
  arr_impact,
  audit_period,
  caa_activity_type,
  is_external_audit,
  gitlab_assignee,
  gitlab_issue_url,
  priority_level
FROM source