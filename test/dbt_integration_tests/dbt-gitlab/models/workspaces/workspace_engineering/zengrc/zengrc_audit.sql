WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_audit_source') }}

)

SELECT
  audit_code,
  audit_created_at,
  audidt_description,
  audit_end_date,
  audit_id,
  program_id,
  audit_report_period_end_date,
  audit_report_period_start_date,
  audit_start_date,
  audit_status,
  has_external_attachments,
  has_external_comments,
  audit_title,
  zengrc_object_type,
  audit_uploaded_at,
  audit_loaded_at,
  audit_category,
  audit_completion_date,
  delegated_testing_owner,
  documentation_due_date,
  escalation_date,
  gitlab_assignee,
  inherent_risk,
  period_completed,
  period_created,
  residual_risk,
  system_effectiveness_rating,
  system_tier_level
FROM source