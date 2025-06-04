{{ config(
    materialized = 'table',
    tags = ["mnpi_exception", "product"]
) }} 

WITH unify AS (

  SELECT
    f.value::VARCHAR                                                            AS dim_crm_account_id,
    rpt_behavior_code_suggestion_outcome.gitlab_global_user_id,
    rpt_behavior_code_suggestion_outcome.requested_at                           AS _datetime,
    'gitlab_ide_extension'                                                      AS app_id,
    rpt_behavior_code_suggestion_outcome.language,
    rpt_behavior_code_suggestion_outcome.delivery_type,
    rpt_behavior_code_suggestion_outcome.product_deployment_type,
    rpt_behavior_code_suggestion_outcome.is_direct_connection,
    rpt_behavior_code_suggestion_outcome.suggestion_id,
    NULL                                                                        AS behavior_structured_event_pk,
    rpt_behavior_code_suggestion_outcome.suggestion_source,
    rpt_behavior_code_suggestion_outcome.model_engine,
    rpt_behavior_code_suggestion_outcome.model_name,
    rpt_behavior_code_suggestion_outcome.extension_name,
    rpt_behavior_code_suggestion_outcome.extension_version,
    rpt_behavior_code_suggestion_outcome.ide_name,
    rpt_behavior_code_suggestion_outcome.ide_version,
    rpt_behavior_code_suggestion_outcome.ide_vendor,
    rpt_behavior_code_suggestion_outcome.language_server_version,
    rpt_behavior_code_suggestion_outcome.has_advanced_context,
    rpt_behavior_code_suggestion_outcome.is_streaming,
    rpt_behavior_code_suggestion_outcome.is_invoked,
    rpt_behavior_code_suggestion_outcome.options_count,
    rpt_behavior_code_suggestion_outcome.display_time_in_ms,
    rpt_behavior_code_suggestion_outcome.load_time_in_ms,
    rpt_behavior_code_suggestion_outcome.time_to_show_in_ms,
    rpt_behavior_code_suggestion_outcome.suggestion_outcome,
    rpt_behavior_code_suggestion_outcome.was_accepted,
    rpt_behavior_code_suggestion_outcome.was_cancelled,
    rpt_behavior_code_suggestion_outcome.was_error,
    rpt_behavior_code_suggestion_outcome.was_loaded,
    rpt_behavior_code_suggestion_outcome.was_not_provided,
    rpt_behavior_code_suggestion_outcome.was_rejected,
    rpt_behavior_code_suggestion_outcome.was_requested,
    rpt_behavior_code_suggestion_outcome.was_shown,
    rpt_behavior_code_suggestion_outcome.was_stream_completed,
    rpt_behavior_code_suggestion_outcome.was_stream_started,
    rpt_behavior_code_suggestion_outcome.input_tokens,
    rpt_behavior_code_suggestion_outcome.output_tokens
  FROM {{ ref('rpt_behavior_code_suggestion_outcome') }},
    LATERAL FLATTEN(input => rpt_behavior_code_suggestion_outcome.dim_crm_account_ids::ARRAY) AS f
  WHERE f.value IS NOT NULL
    AND rpt_behavior_code_suggestion_outcome.requested_at >= '2024-06-26'


  UNION ALL

  SELECT
    f.value::VARCHAR                                                            AS dim_crm_account_id,
    rpt_behavior_code_suggestion_gateway_request.gitlab_global_user_id,
    rpt_behavior_code_suggestion_gateway_request.behavior_at,
    rpt_behavior_code_suggestion_gateway_request.app_id,
    rpt_behavior_code_suggestion_gateway_request.language,
    rpt_behavior_code_suggestion_gateway_request.delivery_type,
    rpt_behavior_code_suggestion_gateway_request.product_deployment_type,
    rpt_behavior_code_suggestion_gateway_request.is_direct_connection,
    NULL                                                                        AS suggestion_id,
    rpt_behavior_code_suggestion_gateway_request.behavior_structured_event_pk,
    NULL                                                                        AS suggestion_source,
    NULL                                                                        AS model_engine,
    NULL                                                                        AS model_name,
    NULL                                                                        AS extension_name,
    NULL                                                                        AS extension_version,
    NULL                                                                        AS ide_name,
    NULL                                                                        AS ide_version,
    NULL                                                                        AS ide_vendor,
    NULL                                                                        AS language_server_version,
    NULL                                                                        AS has_advanced_context,
    NULL                                                                        AS is_streaming,
    NULL                                                                        AS is_invoked,
    NULL                                                                        AS options_count,
    NULL                                                                        AS display_time_in_ms,
    NULL                                                                        AS load_time_in_ms,
    NULL                                                                        AS time_to_show_in_ms,
    NULL                                                                        AS suggestion_outcome,
    NULL                                                                        AS was_accepted,
    NULL                                                                        AS was_cancelled,
    NULL                                                                        AS was_error,
    NULL                                                                        AS was_loaded,
    NULL                                                                        AS was_not_provided,
    NULL                                                                        AS was_rejected,
    NULL                                                                        AS was_requested,
    NULL                                                                        AS was_shown,
    NULL                                                                        AS was_stream_completed,
    NULL                                                                        AS was_stream_started,
    NULL                                                                        AS input_tokens,
    NULL                                                                        AS output_tokens
  FROM {{ ref('rpt_behavior_code_suggestion_gateway_request') }},
    LATERAL FLATTEN(input => rpt_behavior_code_suggestion_gateway_request.dim_crm_account_ids::ARRAY) AS f
  WHERE f.value IS NOT NULL
    AND rpt_behavior_code_suggestion_gateway_request.behavior_at >= '2024-06-26'

)

SELECT
  dim_crm_account.crm_account_name,
  unify.*
FROM unify
LEFT JOIN {{ ref('dim_crm_account') }}
  ON unify.dim_crm_account_id = dim_crm_account.dim_crm_account_id
