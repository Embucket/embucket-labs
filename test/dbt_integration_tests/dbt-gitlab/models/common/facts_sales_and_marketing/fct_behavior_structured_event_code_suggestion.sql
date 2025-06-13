{{
  config(
    materialized='incremental',
    unique_key='behavior_structured_event_pk',
    tags=["mnpi_exception", "product"],
    on_schema_change='sync_all_columns'
  )
}}


{{ simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('bdg_namespace_order_subscription', 'bdg_namespace_order_subscription'),
    ('prep_subscription', 'prep_subscription'),
    ('zuora_product_rate_plan_source', 'zuora_product_rate_plan_source'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_crm_account', 'prep_crm_account'),
    ('dim_installation', 'dim_installation'),
    ('fct_ping_instance_metric', 'fct_ping_instance_metric')
]) }},

code_suggestion_context AS (

  SELECT
    fct_behavior_structured_event.behavior_structured_event_pk,
    fct_behavior_structured_event.behavior_at,
    fct_behavior_structured_event.code_suggestions_context,
    fct_behavior_structured_event.model_engine,
    fct_behavior_structured_event.model_name,
    fct_behavior_structured_event.prefix_length,
    fct_behavior_structured_event.suffix_length,
    fct_behavior_structured_event.language,
    fct_behavior_structured_event.delivery_type AS code_suggestions_context_delivery_type,
    fct_behavior_structured_event.api_status_code,
    fct_behavior_structured_event.duo_namespace_ids,
    fct_behavior_structured_event.saas_namespace_ids,
    fct_behavior_structured_event.namespace_ids,
    fct_behavior_structured_event.dim_instance_id,
    fct_behavior_structured_event.host_name,
    fct_behavior_structured_event.is_streaming,
    fct_behavior_structured_event.gitlab_global_user_id,
    fct_behavior_structured_event.suggestion_source,
    fct_behavior_structured_event.is_invoked,
    fct_behavior_structured_event.options_count,
    fct_behavior_structured_event.accepted_option,
    fct_behavior_structured_event.has_advanced_context,
    fct_behavior_structured_event.is_direct_connection,
    fct_behavior_structured_event.gsc_instance_version,
    fct_behavior_structured_event.total_context_size_bytes,
    fct_behavior_structured_event.content_above_cursor_size_bytes,
    fct_behavior_structured_event.content_below_cursor_size_bytes,
    fct_behavior_structured_event.context_items,
    fct_behavior_structured_event.context_items_count,
    fct_behavior_structured_event.input_tokens,
    fct_behavior_structured_event.output_tokens,
    fct_behavior_structured_event.context_tokens_sent,
    fct_behavior_structured_event.context_tokens_used,
    fct_behavior_structured_event.debounce_interval,
    fct_behavior_structured_event.region,
    fct_behavior_structured_event.resolution_strategy
  FROM fct_behavior_structured_event
  WHERE fct_behavior_structured_event.behavior_at >= '2023-08-01' -- no events added to context before Aug 2023
    AND fct_behavior_structured_event.has_code_suggestions_context = TRUE
  {% if is_incremental() %}

      AND fct_behavior_structured_event.behavior_at >= (SELECT MAX(behavior_at) FROM {{ this }})

  {% endif %}

),

flattened_namespaces AS (

  SELECT
    code_suggestion_context.behavior_structured_event_pk,
    flattened_namespace.value::VARCHAR AS namespace_id
  FROM code_suggestion_context,
    LATERAL FLATTEN(input => TRY_PARSE_JSON(code_suggestion_context.namespace_ids)) AS flattened_namespace
  WHERE code_suggestion_context.namespace_ids IS NOT NULL
    AND code_suggestion_context.namespace_ids != '[]'

),

deduped_namespace_bdg AS (

  /*
  Limit the subscriptions we connect to code suggestions to only the paid tiers and filter to the most recent subscription version.
  This will give us the most recent subscription-namespace connection available in the data.
  */

  SELECT
    bdg_namespace_order_subscription.dim_subscription_id AS dim_latest_subscription_id,
    bdg_namespace_order_subscription.subscription_name   AS latest_subscription_name,
    bdg_namespace_order_subscription.dim_crm_account_id,
    bdg_namespace_order_subscription.dim_billing_account_id,
    bdg_namespace_order_subscription.dim_namespace_id
  FROM bdg_namespace_order_subscription
  INNER JOIN prep_subscription
    ON bdg_namespace_order_subscription.dim_subscription_id = prep_subscription.dim_subscription_id
  LEFT JOIN zuora_product_rate_plan_source AS order_product_rate_plan
    ON bdg_namespace_order_subscription.product_rate_plan_id_order = order_product_rate_plan.product_rate_plan_id
  LEFT JOIN zuora_product_rate_plan_source AS subscription_product_rate_plan
    ON bdg_namespace_order_subscription.product_rate_plan_id_subscription = subscription_product_rate_plan.product_rate_plan_id
  WHERE bdg_namespace_order_subscription.product_tier_name_subscription IN ('SaaS - Bronze', 'SaaS - Ultimate', 'SaaS - Premium')
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bdg_namespace_order_subscription.dim_namespace_id ORDER BY prep_subscription.subscription_version DESC) = 1

),

prep_namespace_w_bdg AS (

  SELECT
    prep_namespace.dim_namespace_id,
    prep_namespace.dim_product_tier_id AS dim_latest_product_tier_id,
    deduped_namespace_bdg.dim_latest_subscription_id,
    deduped_namespace_bdg.dim_crm_account_id,
    deduped_namespace_bdg.dim_billing_account_id,
    deduped_namespace_bdg.latest_subscription_name
  FROM deduped_namespace_bdg
  INNER JOIN prep_namespace
    ON deduped_namespace_bdg.dim_namespace_id = prep_namespace.dim_namespace_id

),

code_suggestions_with_ultimate_parent_namespaces_and_crm_accounts AS (

  SELECT
    flattened_namespaces.behavior_structured_event_pk,
    flattened_namespaces.namespace_id,
    prep_namespace.ultimate_parent_namespace_id,
    prep_namespace_w_bdg.latest_subscription_name AS subscription_name,
    prep_namespace.namespace_is_internal,
    prep_crm_account.dim_crm_account_id,
    prep_crm_account.dim_parent_crm_account_id,
    prep_crm_account.crm_account_name,
    prep_crm_account.parent_crm_account_name
  FROM flattened_namespaces
  LEFT JOIN prep_namespace
    ON flattened_namespaces.namespace_id = prep_namespace.namespace_id
  LEFT JOIN prep_namespace_w_bdg
    ON flattened_namespaces.namespace_id = prep_namespace_w_bdg.dim_namespace_id
  LEFT JOIN prep_crm_account
    ON prep_namespace_w_bdg.dim_crm_account_id = prep_crm_account.dim_crm_account_id

),

code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas AS (

  SELECT
    behavior_structured_event_pk,
    ARRAY_AGG(DISTINCT ultimate_parent_namespace_id) WITHIN GROUP (ORDER BY ultimate_parent_namespace_id) AS ultimate_parent_namespace_ids,
    ARRAY_AGG(DISTINCT subscription_name) WITHIN GROUP (ORDER BY subscription_name ASC)                   AS subscription_names,
    ARRAY_AGG(DISTINCT dim_crm_account_id) WITHIN GROUP (ORDER BY dim_crm_account_id ASC)                 AS dim_crm_account_ids,
    ARRAY_AGG(DISTINCT dim_parent_crm_account_id) WITHIN GROUP (ORDER BY dim_parent_crm_account_id ASC)   AS dim_parent_crm_account_ids,
    ARRAY_AGG(DISTINCT crm_account_name) WITHIN GROUP (ORDER BY crm_account_name)                         AS crm_account_names,
    ARRAY_AGG(DISTINCT parent_crm_account_name) WITHIN GROUP (ORDER BY parent_crm_account_name)           AS parent_crm_account_names,
    ARRAY_SIZE(ultimate_parent_namespace_ids)                                                             AS count_ultimate_parent_namespace_ids,
    ARRAY_SIZE(subscription_names)                                                                        AS count_subscription_names,
    ARRAY_SIZE(dim_crm_account_ids)                                                                       AS count_dim_crm_account_ids,
    ARRAY_SIZE(dim_parent_crm_account_ids)                                                                AS count_dim_parent_crm_account_ids,
    ARRAY_SIZE(crm_account_names)                                                                         AS count_crm_account_names,
    ARRAY_SIZE(parent_crm_account_names)                                                                  AS count_parent_crm_account_names,
    MAX(namespace_is_internal)                                                                            AS namespace_is_internal
  FROM code_suggestions_with_ultimate_parent_namespaces_and_crm_accounts
  GROUP BY ALL

),

context_with_installation_id AS (

  SELECT
    code_suggestion_context.behavior_structured_event_pk,
    code_suggestion_context.dim_instance_id,
    dim_installation.dim_installation_id,
    dim_installation.host_name,
    dim_installation.product_delivery_type,
    dim_installation.product_deployment_type,
    dim_installation.is_internal
  FROM code_suggestion_context
  LEFT JOIN dim_installation
    ON code_suggestion_context.dim_instance_id = dim_installation.dim_instance_id
  WHERE code_suggestion_context.dim_instance_id IS NOT NULL
    AND (
      code_suggestion_context.host_name IS NULL
      OR code_suggestion_context.host_name = ''
    )

  UNION

  SELECT
    code_suggestion_context.behavior_structured_event_pk,
    code_suggestion_context.dim_instance_id,
    dim_installation.dim_installation_id,
    dim_installation.host_name,
    dim_installation.product_delivery_type,
    dim_installation.product_deployment_type,
    dim_installation.is_internal
  FROM code_suggestion_context
  LEFT JOIN dim_installation
    ON code_suggestion_context.dim_instance_id = dim_installation.dim_instance_id
      AND code_suggestion_context.host_name = dim_installation.host_name
  WHERE code_suggestion_context.dim_instance_id IS NOT NULL
    AND code_suggestion_context.host_name IS NOT NULL
    AND code_suggestion_context.host_name != ''

),

bdg_latest_instance_subscription AS (

  SELECT
    fct_ping_instance_metric.dim_installation_id,
    fct_ping_instance_metric.dim_subscription_id,
    prep_subscription.subscription_name,
    prep_subscription.dim_crm_account_id,
    prep_crm_account.dim_parent_crm_account_id,
    prep_crm_account.crm_account_name,
    prep_crm_account.parent_crm_account_name
  FROM fct_ping_instance_metric
  LEFT JOIN prep_subscription
    ON fct_ping_instance_metric.dim_subscription_id = prep_subscription.dim_subscription_id
  LEFT JOIN prep_crm_account
    ON prep_subscription.dim_crm_account_id = prep_crm_account.dim_crm_account_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY fct_ping_instance_metric.dim_installation_id ORDER BY fct_ping_instance_metric.dim_ping_date_id DESC) = 1

),

code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm AS (

  SELECT
    context_with_installation_id.behavior_structured_event_pk,
    ARRAY_AGG(DISTINCT context_with_installation_id.dim_installation_id) WITHIN GROUP (ORDER BY context_with_installation_id.dim_installation_id ASC)                     AS dim_installation_ids,
    ARRAY_AGG(DISTINCT context_with_installation_id.host_name) WITHIN GROUP (ORDER BY context_with_installation_id.host_name)                                             AS host_names,
    ARRAY_AGG(DISTINCT context_with_installation_id.product_delivery_type) WITHIN GROUP (ORDER BY context_with_installation_id.product_delivery_type ASC)                 AS product_delivery_types,
    ARRAY_AGG(DISTINCT context_with_installation_id.product_deployment_type) WITHIN GROUP (ORDER BY context_with_installation_id.product_deployment_type ASC)             AS product_deployment_types,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.subscription_name) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.subscription_name ASC)                 AS subscription_names,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.dim_crm_account_id) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.dim_crm_account_id ASC)               AS dim_crm_account_ids,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.dim_parent_crm_account_id) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.dim_parent_crm_account_id ASC) AS dim_parent_crm_account_ids,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.crm_account_name) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.crm_account_name)                       AS crm_account_names,
    ARRAY_AGG(DISTINCT bdg_latest_instance_subscription.parent_crm_account_name) WITHIN GROUP (ORDER BY bdg_latest_instance_subscription.parent_crm_account_name)         AS parent_crm_account_names,
    COALESCE(context_with_installation_id.is_internal, FALSE)                                                                                                             AS installation_is_internal,
    ARRAY_SIZE(dim_installation_ids)                                                                                                                                      AS count_dim_installation_ids,
    ARRAY_SIZE(subscription_names)                                                                                                                                        AS count_subscription_names,
    ARRAY_SIZE(dim_crm_account_ids)                                                                                                                                       AS count_dim_crm_account_ids,
    ARRAY_SIZE(dim_parent_crm_account_ids)                                                                                                                                AS count_dim_parent_crm_account_ids,
    ARRAY_SIZE(crm_account_names)                                                                                                                                         AS count_crm_account_names,
    ARRAY_SIZE(parent_crm_account_names)                                                                                                                                  AS count_parent_crm_account_names,
    ARRAY_SIZE(host_names)                                                                                                                                                AS count_host_names,
    ARRAY_SIZE(product_delivery_types)                                                                                                                                    AS count_product_delivery_types,
    ARRAY_SIZE(product_deployment_types)                                                                                                                                  AS count_product_deployment_types
  FROM context_with_installation_id
  LEFT JOIN bdg_latest_instance_subscription
    ON context_with_installation_id.dim_installation_id = bdg_latest_instance_subscription.dim_installation_id
  GROUP BY ALL

),

combined AS (

  SELECT
    code_suggestion_context.*,
    COALESCE(
      IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_product_delivery_types = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.product_delivery_types, 0)::VARCHAR, NULL),
      code_suggestion_context.code_suggestions_context_delivery_type
    )                                                                                                                                                                                                                                    AS delivery_type,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.ultimate_parent_namespace_ids, [])                                                                                                                         AS ultimate_parent_namespace_ids,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.subscription_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.subscription_names), [])                                       AS subscription_names,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_crm_account_ids, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_crm_account_ids), [])                                     AS dim_crm_account_ids,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_parent_crm_account_ids, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_parent_crm_account_ids), [])                       AS dim_parent_crm_account_ids,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.crm_account_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.crm_account_names), [])                                         AS crm_account_names,
    COALESCE(COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.parent_crm_account_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.parent_crm_account_names), [])                           AS parent_crm_account_names,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_installation_ids, [])                                                                                                                                    AS dim_installation_ids,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.host_names, [])                                                                                                                                              AS host_names,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.product_delivery_types, [])                                                                                                                                  AS product_delivery_types,
    COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.product_deployment_types, [])                                                                                                                                AS product_deployment_types,
    IFF(
      COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_dim_crm_account_ids, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_dim_crm_account_ids) = 1,
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_crm_account_ids, 0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_crm_account_ids, 0))::VARCHAR, NULL
    )                                                                                                                                                                                                                                    AS dim_crm_account_id,
    IFF(
      COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_dim_parent_crm_account_ids, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_dim_parent_crm_account_ids) = 1,
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.dim_parent_crm_account_ids, 0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_parent_crm_account_ids, 0))::VARCHAR, NULL
    )                                                                                                                                                                                                                                    AS dim_parent_crm_account_id,
    IFF(
      COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_subscription_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_subscription_names) = 1,
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.subscription_names, 0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.subscription_names, 0))::VARCHAR, NULL
    )                                                                                                                                                                                                                                    AS subscription_name,
    IFF(
      COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_crm_account_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_crm_account_names) = 1,
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.crm_account_names, 0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.crm_account_names, 0))::VARCHAR, NULL
    )                                                                                                                                                                                                                                    AS crm_account_name,
    IFF(
      COALESCE(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_parent_crm_account_names, code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_parent_crm_account_names) = 1,
      COALESCE(GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.parent_crm_account_names, 0), GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.parent_crm_account_names, 0))::VARCHAR, NULL
    )                                                                                                                                                                                                                                    AS parent_crm_account_name,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.count_ultimate_parent_namespace_ids = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.ultimate_parent_namespace_ids, 0)::VARCHAR, NULL) AS ultimate_parent_namespace_id,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_dim_installation_ids = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.dim_installation_ids, 0)::VARCHAR, NULL)                       AS dim_installation_id,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_host_names = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.host_names, 0)::VARCHAR, NULL)                                           AS installation_host_name,
    IFF(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.count_product_deployment_types = 1, GET(code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.product_deployment_types, 0)::VARCHAR, NULL)               AS product_deployment_type,
    code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.namespace_is_internal,
    code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.installation_is_internal
  FROM code_suggestion_context
  LEFT JOIN code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas
    ON code_suggestion_context.behavior_structured_event_pk = code_suggestions_with_multiple_ultimate_parent_crm_accounts_saas.behavior_structured_event_pk
  LEFT JOIN code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm
    ON code_suggestion_context.behavior_structured_event_pk = code_suggestions_with_multiple_ultimate_parent_crm_accounts_sm.behavior_structured_event_pk
)

{{ dbt_audit(
    cte_ref="combined",
    created_by="@michellecooper",
    updated_by="@lisvinueza",
    created_date="2024-04-09",
    updated_date="2025-03-04"
) }}
