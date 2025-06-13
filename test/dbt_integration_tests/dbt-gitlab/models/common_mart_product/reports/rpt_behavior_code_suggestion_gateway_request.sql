{{ config(

    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=['mnpi_exception','product'],
    on_schema_change='sync_all_columns',
    cluster_by=['behavior_at::DATE']
  )

}}

WITH gitlab_ai_gateway_request_events AS (

  SELECT
    --basic event properties
    behavior_structured_event_pk,
    behavior_at,
    behavior_date,
    app_id,
    event_category,
    event_action,

    --basic request properties available on the gateway request
    language,
    delivery_type,
    product_deployment_type,
    prefix_length,
    suffix_length,
    is_direct_connection,
    region,

    --attribution properties
    gitlab_global_user_id,
    dim_instance_id,
    host_name,
    dim_installation_ids,
    namespace_ids,
    ultimate_parent_namespace_ids,
    namespace_is_internal,
    installation_is_internal,
    dim_crm_account_ids,
    crm_account_names,

    --contexts
    contexts,
    code_suggestions_context
  FROM {{ ref('mart_behavior_structured_event_code_suggestion') }}
  WHERE app_id = 'gitlab_ai_gateway'
    AND event_action IN ('suggestion_requested', 'suggestions_requested')
  {% if is_incremental() %}

      AND behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})

    {% endif %}

)

SELECT *
FROM gitlab_ai_gateway_request_events
