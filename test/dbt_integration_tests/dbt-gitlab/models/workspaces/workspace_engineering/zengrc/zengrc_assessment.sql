WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_assessment_source') }}

)

SELECT
  assessment_code,
  assessment_conclusion,
  control_id,
  assessment_created_at,
  assessment_description,
  assessment_end_date,
  assessment_id,
  assessment_start_date,
  assessment_status,
  assessment_title,
  zengrc_object_type,
  assessment_uploaded_at,
  assessment_loaded_at,
  annualized_population_size,
  competency_and_authority_of_control_owners,
  control_health_and_effectiveness_rating_cher,
  control_implementation_statement,
  criteria_for_investigation_and_follow_up,
  customer_hosting_option,
  gitlab_control_family,
  gitlab_control_owner,
  level_of_consistency_and_frequency,
  level_of_judgement_and_aggregation,
  population_date_range,
  population_size,
  purpose_and_appropriateness_of_the_control,
  risk_associated_with_the_control,
  sample_size,
  system_level,
  test_of_design_results,
  test_of_operating_effectiveness_results,
  total_hours_estimate
FROM source