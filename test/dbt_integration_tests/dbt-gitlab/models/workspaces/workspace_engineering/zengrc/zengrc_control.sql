WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_control_source') }}

)

SELECT
  control_code,
  control_created_at,
  control_description,
  control_id,
  control_status,
  control_title,
  zengrc_object_type,
  control_updated_at,
  control_loaded_at,
  application_used,
  average_control_effectiveness_rating,
  control_deployment,
  control_level,
  control_objective,
  control_objective_ref_number,
  control_type,
  coso_components,
  coso_principles,
  financial_cycle,
  financial_statement_assertions,
  gitlab_guidance,
  ia_significance,
  ia_test_plan,
  ipe_report_name,
  ipe_review_parameters,
  management_review_control,
  mitigating_control_activities,
  original_scf_control_language,
  process,
  process_owner_name,
  process_owner_title,
  relative_control_weighting,
  risk_event,
  risk_ref_number,
  sample_control_implementation,
  scf_control_question,
  spreadsheet_name,
  spreadsheet_review_parameters,
  sub_process
FROM source