WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_risk_source') }}

)

SELECT
  risk_code,
  risk_created_at,
  risk_description,
  risk_id,
  risk_vector_score_values,
  risk_status,
  risk_title,
  zengrc_object_type,
  risk_updated_at,
  risk_loaded_at,
  acceptance_of_risk_ownership,
  cia_impact,
  risk_identified_date,
  existing_mitigations,
  interested_parties,
  is_risk_ready_for_review_and_closure,
  risk_owner,
  risk_tier,
  risk_treatment_completion_date,
  risk_treatment_option_selected,
  root_cause,
  threat_source,
  is_tprm_related,
  is_within_risk_appetite
FROM source