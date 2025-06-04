{{ config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['mnpi_exception','product'],
    on_schema_change='sync_all_columns'
  )

}}

{{ simple_cte([
    ('fct_behavior_structured_event_code_suggestion', 'fct_behavior_structured_event_code_suggestion'),
    ('dim_behavior_event', 'dim_behavior_event'),
]) }}

, fct_behavior_structured_event AS (

 SELECT
  {{ dbt_utils.star(from=ref('fct_behavior_structured_event'), except=["CREATED_BY",
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT",
    "MODEL_ENGINE","MODEL_NAME","PREFIX_LENGTH","SUFFIX_LENGTH","LANGUAGE","USER_AGENT","DELIVERY_TYPE","API_STATUS_CODE","NAMESPACE_IDS","DIM_INSTANCE_ID","HOST_NAME"]) }}
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE behavior_at >= '2023-08-25'
    AND has_code_suggestions_context = TRUE
    AND app_id IN ('gitlab_ai_gateway', 'gitlab_ide_extension') --"official" Code Suggestions app_ids
    AND NOT (IFNULL(ide_name, '') = 'Visual Studio Code' AND IFNULL(extension_version, '') = '3.76.0') --exclude IDE events from VS Code extension version 3.76.0 (which sent duplicate events)

),

code_suggestions_context AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_behavior_structured_event_code_suggestion'), except=["CREATED_BY",
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_behavior_structured_event_code_suggestion
  {% if is_incremental() %}

    WHERE behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})

  {% endif %}

),

filtered_code_suggestion_events AS (

  SELECT
    code_suggestions_context.behavior_structured_event_pk,
    code_suggestions_context.behavior_at,
    code_suggestions_context.behavior_at::DATE AS behavior_date,
    fct_behavior_structured_event.app_id,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_label,
    dim_behavior_event.event_property,
    code_suggestions_context.language,
    code_suggestions_context.delivery_type,
    code_suggestions_context.model_engine,
    code_suggestions_context.model_name,
    code_suggestions_context.prefix_length,
    code_suggestions_context.suffix_length,
    code_suggestions_context.api_status_code,
    fct_behavior_structured_event.extension_name,
    fct_behavior_structured_event.extension_version,
    fct_behavior_structured_event.ide_name,
    fct_behavior_structured_event.ide_vendor,
    fct_behavior_structured_event.ide_version,
    fct_behavior_structured_event.language_server_version,
    fct_behavior_structured_event.contexts,
    code_suggestions_context.code_suggestions_context,
    fct_behavior_structured_event.ide_extension_version_context,
    fct_behavior_structured_event.has_code_suggestions_context,
    fct_behavior_structured_event.has_ide_extension_version_context,
    code_suggestions_context.dim_instance_id,
    code_suggestions_context.host_name,
    code_suggestions_context.is_streaming,
    code_suggestions_context.gitlab_global_user_id,
    code_suggestions_context.suggestion_source,
    code_suggestions_context.is_invoked,
    code_suggestions_context.options_count,
    code_suggestions_context.accepted_option,
    code_suggestions_context.has_advanced_context,
    code_suggestions_context.is_direct_connection,
    code_suggestions_context.namespace_ids,
    code_suggestions_context.ultimate_parent_namespace_ids,
    code_suggestions_context.dim_installation_ids,
    code_suggestions_context.host_names,
    code_suggestions_context.subscription_names,
    code_suggestions_context.dim_crm_account_ids,
    code_suggestions_context.crm_account_names,
    code_suggestions_context.dim_parent_crm_account_ids,
    code_suggestions_context.parent_crm_account_names,
    code_suggestions_context.dim_crm_account_id,
    code_suggestions_context.crm_account_name,
    code_suggestions_context.dim_parent_crm_account_id,
    code_suggestions_context.parent_crm_account_name,
    code_suggestions_context.subscription_name,
    code_suggestions_context.ultimate_parent_namespace_id,
    code_suggestions_context.dim_installation_id,
    code_suggestions_context.installation_host_name,
    code_suggestions_context.product_deployment_type,
    code_suggestions_context.namespace_is_internal,
    code_suggestions_context.installation_is_internal,
    code_suggestions_context.gsc_instance_version,
    code_suggestions_context.total_context_size_bytes,
    code_suggestions_context.content_above_cursor_size_bytes,
    code_suggestions_context.content_below_cursor_size_bytes,
    code_suggestions_context.context_items,
    code_suggestions_context.context_items_count,
    code_suggestions_context.input_tokens,
    code_suggestions_context.output_tokens,
    code_suggestions_context.context_tokens_sent,
    code_suggestions_context.context_tokens_used,
    code_suggestions_context.debounce_interval,
    code_suggestions_context.region,
    code_suggestions_context.resolution_strategy
  FROM code_suggestions_context
  INNER JOIN fct_behavior_structured_event
    ON code_suggestions_context.behavior_structured_event_pk = fct_behavior_structured_event.behavior_structured_event_pk
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk

)

SELECT *
FROM filtered_code_suggestion_events
