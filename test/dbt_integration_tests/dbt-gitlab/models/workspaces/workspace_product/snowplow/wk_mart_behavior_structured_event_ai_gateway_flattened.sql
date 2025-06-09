{{ config(
    materialized="table",
    tags=["product", "mnpi_exception"],
    cluster_by=['behavior_at::DATE']
) }}

{{ simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('dim_behavior_event', 'dim_behavior_event'),
    ('dim_installation', 'dim_installation'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_app_release_major_minor', 'dim_app_release_major_minor'),
    ('wk_ping_installation_latest', 'wk_ping_installation_latest'),
    ('map_namespace_subscription_product', 'map_namespace_subscription_product'),
    ('map_installation_subscription_product','map_installation_subscription_product'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_crm_account', 'dim_crm_account'),
    ('prep_namespace_order_trial', 'prep_namespace_order_trial'),
    ('prep_trial', 'prep_trial'),
    ('dim_subscription', 'dim_subscription'),
    ('customers_db_trials', 'customers_db_trials'),
    ('dim_ai_gateway_unit_primitives', 'dim_ai_gateway_unit_primitives')
    ])
}}

, up_events AS (

  SELECT
    fct_behavior_structured_event.* EXCLUDE(feature_category),
    fct_behavior_structured_event.feature_category             AS feature_category_at_event_time,
    dim_behavior_event.event,
    dim_behavior_event.event_name,
    dim_behavior_event.platform,
    dim_behavior_event.environment,
    dim_behavior_event.event_category,
    dim_behavior_event.event_action,
    dim_behavior_event.event_property,
    dim_behavior_event.unit_primitive,
    dim_ai_gateway_unit_primitives.feature_category,
    dim_ai_gateway_unit_primitives.engineering_group,
    dim_ai_gateway_unit_primitives.backend_services
  FROM fct_behavior_structured_event
  LEFT JOIN dim_behavior_event
    ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
  LEFT JOIN dim_ai_gateway_unit_primitives
    ON dim_ai_gateway_unit_primitives.unit_primitive_name = dim_behavior_event.unit_primitive

/* 
Filters:
- first date after events were implemented and pseudonymization was fixed in https://gitlab.com/gitlab-org/analytics-section/analytics-instrumentation/snowplow-pseudonymization/-/merge_requests/27
- event_action indicates it is a unit primitive
- event occurred in AI Gateway
*/
  WHERE behavior_at >= '2024-08-03'
    AND event_action LIKE 'request_%'
    AND app_id = 'gitlab_ai_gateway'

), sm_dedicated_duo_trials AS (

  SELECT 
    customers_db_trials.subscription_name,
    customers_db_trials.start_date          AS trial_start_date,
    customers_db_trials.end_date            AS trial_end_date,
    customers_db_trials.product_rate_plan_id,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2)) AS product_category_short,
    CASE
      WHEN product_category_short = 'GitLab Duo Pro'
        THEN 'Duo Pro Trial'
      WHEN product_category_short = 'GitLab Duo Enterprise'
        THEN 'Duo Enterprise Trial'
     END                                                                                    AS product_rate_plan_category_general,
    dim_product_detail.product_rate_plan_name,
    dim_subscription.dim_subscription_id_original,
    customers_db_trials.subscription_name                                                   AS enabled_by_sm_dedicated_duo_trial_subscription_name
  FROM customers_db_trials
  LEFT JOIN dim_product_detail
    ON customers_db_trials.product_rate_plan_id = dim_product_detail.product_rate_plan_id
  LEFT JOIN dim_subscription
    ON customers_db_trials.subscription_name = dim_subscription.subscription_name
     AND dim_subscription.subscription_version = 1
  WHERE dim_product_detail.product_rate_plan_name ILIKE '%duo%'

), dotcom_duo_trials AS (

  SELECT DISTINCT
    dim_namespace.dim_namespace_id,
    CASE
      WHEN prep_namespace_order_trial.trial_type = 2
        THEN 'Duo Pro Trial'
      WHEN prep_namespace_order_trial.trial_type IN (3,5,6)
        THEN 'Duo Enterprise Trial'
    END                                                               AS product_rate_plan_category_general,
    prep_namespace_order_trial.order_start_date                       AS trial_start_date,
    MAX(prep_trial.trial_end_date)                                    AS latest_trial_end_date
  FROM prep_namespace_order_trial
  LEFT JOIN prep_trial
    ON prep_namespace_order_trial.dim_namespace_id = prep_trial.dim_namespace_id
    AND prep_namespace_order_trial.order_start_date = prep_trial.trial_start_date
    AND prep_trial.product_rate_plan_id LIKE '%duo%'
  INNER JOIN dim_namespace
    ON prep_namespace_order_trial.dim_namespace_id = dim_namespace.dim_namespace_id
  WHERE prep_namespace_order_trial.trial_type IN (2,3,5,6)
  GROUP BY ALL

), flattened AS (

  SELECT 
    up_events.*,
    flattened_namespace.value::VARCHAR AS enabled_by_namespace_id
  FROM up_events,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(up_events.gsc_feature_enabled_by_namespace_ids), outer => TRUE) AS flattened_namespace
    
), flattened_with_installation_id AS (

  SELECT
    flattened.*,
    dim_installation.dim_installation_id,
    dim_installation.product_delivery_type      AS enabled_by_product_delivery_type,
    dim_installation.product_deployment_type    AS enabled_by_product_deployment_type
  FROM flattened
  LEFT JOIN dim_installation
    ON flattened.dim_instance_id = dim_installation.dim_instance_id
      AND flattened.host_name = dim_installation.host_name

), installation_sub_product AS (

  SELECT 
    map_installation_subscription_product.date_actual,
    map_installation_subscription_product.dim_subscription_id,
    map_installation_subscription_product.dim_subscription_id_original,
    map_installation_subscription_product.dim_installation_id,
    map_installation_subscription_product.dim_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_product_detail.*,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2))             AS product_rate_plan_category_general
  FROM map_installation_subscription_product
  LEFT JOIN dim_product_detail
    ON map_installation_subscription_product.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN dim_crm_account
    ON map_installation_subscription_product.dim_crm_account_id = dim_crm_account.dim_crm_account_id

), installation_subscription AS (

  SELECT DISTINCT 
    date_actual,
    dim_installation_id,
    ARRAY_AGG(DISTINCT installation_sub_product.dim_subscription_id) WITHIN GROUP (ORDER BY installation_sub_product.dim_subscription_id ASC)                             AS dim_subscription_ids,
    ARRAY_AGG(DISTINCT installation_sub_product.dim_subscription_id_original) WITHIN GROUP (ORDER BY installation_sub_product.dim_subscription_id_original)               AS dim_subscription_ids_original,
    ARRAY_AGG(DISTINCT installation_sub_product.dim_crm_account_id) WITHIN GROUP (ORDER BY installation_sub_product.dim_crm_account_id ASC)                               AS dim_crm_account_ids,
    ARRAY_AGG(DISTINCT installation_sub_product.crm_account_name) WITHIN GROUP (ORDER BY installation_sub_product.crm_account_name ASC)                                   AS crm_account_names,
    ARRAY_AGG(DISTINCT installation_sub_product.product_rate_plan_category_general) WITHIN GROUP (ORDER BY installation_sub_product.product_rate_plan_category_general)   AS product_categories,
    ARRAY_AGG(DISTINCT installation_sub_product.product_tier_name_short) WITHIN GROUP (ORDER BY installation_sub_product.product_tier_name_short ASC)                     AS product_tier_names,
    MAX(installation_sub_product.is_oss_or_edu_rate_plan)                                                                                                                 AS oss_or_edu_rate_plans
  FROM installation_sub_product
  WHERE product_category = 'Base Products'
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

), add_on_installation_sub_product AS (

  SELECT DISTINCT 
    date_actual,
    dim_installation_id,
    ARRAY_AGG(DISTINCT installation_sub_product.dim_subscription_id) WITHIN GROUP (ORDER BY installation_sub_product.dim_subscription_id ASC)                                 AS add_on_dim_subscription_ids,
    ARRAY_AGG(DISTINCT installation_sub_product.dim_crm_account_id) WITHIN GROUP (ORDER BY installation_sub_product.dim_crm_account_id ASC)                                   AS add_on_dim_crm_account_ids,
    ARRAY_AGG(DISTINCT installation_sub_product.crm_account_name) WITHIN GROUP (ORDER BY installation_sub_product.crm_account_name ASC)                                       AS add_on_crm_account_names,
    ARRAY_AGG(DISTINCT installation_sub_product.product_rate_plan_category_general) WITHIN GROUP (ORDER BY installation_sub_product.product_rate_plan_category_general ASC)   AS add_on_product_categories
  FROM installation_sub_product
  WHERE product_category = 'Add On Services'
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

), add_on_sm_trial AS (

  SELECT
    date_actual,
    dim_installation_id,
    ARRAY_AGG(DISTINCT sm_dedicated_duo_trials.product_rate_plan_category_general) WITHIN GROUP (ORDER BY sm_dedicated_duo_trials.product_rate_plan_category_general ASC)   AS add_on_trial_product_categories
  FROM installation_sub_product
  LEFT JOIN sm_dedicated_duo_trials
    ON installation_sub_product.dim_subscription_id_original = sm_dedicated_duo_trials.dim_subscription_id_original
      AND installation_sub_product.date_actual BETWEEN sm_dedicated_duo_trials.trial_start_date AND sm_dedicated_duo_trials.trial_end_date
  {{ dbt_utils.group_by(n=2) }}

), namespace_sub_product AS (

  SELECT 
    map_namespace_subscription_product.date_actual,
    map_namespace_subscription_product.dim_subscription_id,
    map_namespace_subscription_product.dim_subscription_id_original,
    map_namespace_subscription_product.dim_namespace_id,
    map_namespace_subscription_product.dim_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_product_detail.*,
    TRIM(SPLIT_PART(SPLIT_PART(dim_product_detail.product_rate_plan_category, '(', 1), '- ', 2))              AS product_rate_plan_category_general
  FROM map_namespace_subscription_product
  LEFT JOIN dim_product_detail
    ON map_namespace_subscription_product.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN dim_crm_account
    ON map_namespace_subscription_product.dim_crm_account_id = dim_crm_account.dim_crm_account_id

), namespace_subscription AS (

  SELECT DISTINCT 
    date_actual,
    dim_namespace_id,
    ARRAY_AGG(DISTINCT namespace_sub_product.dim_subscription_id) WITHIN GROUP (ORDER BY namespace_sub_product.dim_subscription_id ASC)                             AS enabled_by_dim_subscription_ids,
    ARRAY_AGG(DISTINCT namespace_sub_product.dim_subscription_id_original) WITHIN GROUP (ORDER BY namespace_sub_product.dim_subscription_id_original)               AS enabled_by_dim_subscription_ids_original,
    ARRAY_AGG(DISTINCT namespace_sub_product.dim_crm_account_id) WITHIN GROUP (ORDER BY namespace_sub_product.dim_crm_account_id ASC)                               AS enabled_by_dim_crm_account_ids,
    ARRAY_AGG(DISTINCT namespace_sub_product.crm_account_name) WITHIN GROUP (ORDER BY namespace_sub_product.crm_account_name ASC)                                   AS enabled_by_crm_account_names,
    ARRAY_AGG(DISTINCT namespace_sub_product.product_rate_plan_category_general) WITHIN GROUP (ORDER BY namespace_sub_product.product_rate_plan_category_general)   AS enabled_by_product_categories,
    ARRAY_AGG(DISTINCT namespace_sub_product.product_tier_name_short) WITHIN GROUP (ORDER BY namespace_sub_product.product_tier_name_short ASC)                     AS enabled_by_product_tier_names,
    MAX(namespace_sub_product.is_oss_or_edu_rate_plan)                                                                                                              AS enabled_by_oss_or_edu_rate_plan
  FROM namespace_sub_product
  WHERE product_category = 'Base Products'
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

), add_on_namespace_sub_product AS (

  SELECT DISTINCT 
    date_actual,
    dim_namespace_id,
    ARRAY_AGG(DISTINCT namespace_sub_product.dim_subscription_id) WITHIN GROUP (ORDER BY namespace_sub_product.dim_subscription_id ASC)                                 AS enabled_by_add_on_dim_subscription_ids,
    ARRAY_AGG(DISTINCT namespace_sub_product.dim_crm_account_id) WITHIN GROUP (ORDER BY namespace_sub_product.dim_crm_account_id ASC)                                   AS enabled_by_add_on_dim_crm_account_ids,
    ARRAY_AGG(DISTINCT namespace_sub_product.crm_account_name) WITHIN GROUP (ORDER BY namespace_sub_product.crm_account_name ASC)                                       AS enabled_by_add_on_crm_account_names,
    ARRAY_AGG(DISTINCT namespace_sub_product.product_rate_plan_category_general) WITHIN GROUP (ORDER BY namespace_sub_product.product_rate_plan_category_general ASC)   AS enabled_by_add_on_product_categories
  FROM namespace_sub_product
  WHERE product_category = 'Add On Services'
    AND charge_type = 'Recurring'
    AND is_licensed_user = TRUE
  {{ dbt_utils.group_by(n=2) }}

), joined AS (

  SELECT
    -- primary key
    flattened_with_installation_id.behavior_structured_event_pk,

    -- foreign keys
    flattened_with_installation_id.dim_behavior_event_sk,
    dim_app_release_major_minor.dim_app_release_major_minor_sk,
    flattened_with_installation_id.dim_installation_id,
    flattened_with_installation_id.gsc_feature_enabled_by_namespace_ids,
    flattened_with_installation_id.enabled_by_namespace_id,
    dim_namespace.ultimate_parent_namespace_id                                AS enabled_by_ultimate_parent_namespace_id,

    -- dates
    flattened_with_installation_id.behavior_at,

    -- degenerate dimensions
    flattened_with_installation_id.dim_instance_id,
    flattened_with_installation_id.host_name,
    wk_ping_installation_latest.latest_is_internal_installation               AS enabled_by_internal_installation,
    dim_namespace.namespace_is_internal                                       AS enabled_by_internal_namespace,
    flattened_with_installation_id.enabled_by_product_delivery_type,
    flattened_with_installation_id.enabled_by_product_deployment_type,
    flattened_with_installation_id.gitlab_global_user_id,
    flattened_with_installation_id.app_id,

    -- standard context attributes
    flattened_with_installation_id.contexts,
    flattened_with_installation_id.gitlab_standard_context,
    flattened_with_installation_id.gsc_environment,
    flattened_with_installation_id.gsc_source,
    flattened_with_installation_id.delivery_type,
    flattened_with_installation_id.gsc_correlation_id,
    flattened_with_installation_id.gsc_extra,
    flattened_with_installation_id.gsc_instance_version,
    dim_app_release_major_minor.major_minor_version                           AS enabled_by_major_minor_version_at_event_time,
    dim_app_release_major_minor.major_minor_version_num                       AS enabled_by_major_minor_version_num_at_event_time,
    flattened_with_installation_id.interface,
    flattened_with_installation_id.client_type,
    flattened_with_installation_id.client_name,
    flattened_with_installation_id.client_version,
    flattened_with_installation_id.feature_category_at_event_time,
    flattened_with_installation_id.gsc_is_gitlab_team_member,
    flattened_with_installation_id.feature_category,
    flattened_with_installation_id.engineering_group,
    flattened_with_installation_id.backend_services,

    -- user attributes
    flattened_with_installation_id.user_country,
    flattened_with_installation_id.user_timezone_name,

    -- event attributes
    flattened_with_installation_id.event_value,
    flattened_with_installation_id.event_category,
    flattened_with_installation_id.event_action,
    flattened_with_installation_id.event_label,
    flattened_with_installation_id.clean_event_label,
    flattened_with_installation_id.event_property,
    flattened_with_installation_id.unit_primitive,

    -- customer ids/product information
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_dim_subscription_ids) = 0, NULL, namespace_subscription.enabled_by_dim_subscription_ids),
      IFF(ARRAY_SIZE(installation_subscription.dim_subscription_ids) = 0, NULL, installation_subscription.dim_subscription_ids)
      )                                                                          AS enabled_by_dim_subscription_ids_at_event_time,
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_dim_subscription_ids_original) = 0, NULL, namespace_subscription.enabled_by_dim_subscription_ids_original),
      IFF(ARRAY_SIZE(installation_subscription.dim_subscription_ids_original) = 0, NULL, installation_subscription.dim_subscription_ids_original)
      )                                                                          AS enabled_by_dim_subscription_ids_original_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_dim_crm_account_ids) = 0, NULL, namespace_subscription.enabled_by_dim_crm_account_ids),
        IFF(ARRAY_SIZE(installation_subscription.dim_crm_account_ids) = 0, NULL, installation_subscription.dim_crm_account_ids),
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_dim_crm_account_ids) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_dim_crm_account_ids),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_dim_crm_account_ids) = 0, NULL, add_on_installation_sub_product.add_on_dim_crm_account_ids)
        ),
      ' ,'
      )                                                                           AS enabled_by_dim_crm_account_id_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(namespace_subscription.enabled_by_crm_account_names) = 0, NULL, namespace_subscription.enabled_by_crm_account_names),
        IFF(ARRAY_SIZE(installation_subscription.crm_account_names) = 0, NULL, installation_subscription.crm_account_names),
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_crm_account_names) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_crm_account_names),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_crm_account_names) = 0, NULL, add_on_installation_sub_product.add_on_crm_account_names)
        ),
      ' ,'
      )                                                                           AS enabled_by_crm_account_name_at_event_time,
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_product_tier_names) = 0, NULL, namespace_subscription.enabled_by_product_tier_names),
      IFF(ARRAY_SIZE(installation_subscription.product_tier_names) = 0, NULL, installation_subscription.product_tier_names)
      )                                                                           AS enabled_by_product_tier_names_at_event_time,
    COALESCE(
      IFF(ARRAY_SIZE(namespace_subscription.enabled_by_product_categories) = 0, NULL, namespace_subscription.enabled_by_product_categories),
      IFF(ARRAY_SIZE(installation_subscription.product_categories) = 0, NULL, installation_subscription.product_categories)
      )                                                                           AS enabled_by_product_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_dim_subscription_ids) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_dim_subscription_ids),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_dim_subscription_ids) = 0, NULL, add_on_installation_sub_product.add_on_dim_subscription_ids)
        ),
      ' ,'
      )                                                                           AS enabled_by_add_on_dim_subscription_id_at_event_time,
    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(add_on_namespace_sub_product.enabled_by_add_on_product_categories) = 0, NULL, add_on_namespace_sub_product.enabled_by_add_on_product_categories),
        IFF(ARRAY_SIZE(add_on_installation_sub_product.add_on_product_categories) = 0, NULL, add_on_installation_sub_product.add_on_product_categories)
        ),
      ' ,'
      )                                                                           AS enabled_by_add_on_product_at_event_time,

    ARRAY_TO_STRING(
      COALESCE(
        IFF(ARRAY_SIZE(add_on_sm_trial.add_on_trial_product_categories) = 0, NULL, add_on_sm_trial.add_on_trial_product_categories),
        IFF(ARRAY_SIZE(TO_ARRAY(dotcom_duo_trials.product_rate_plan_category_general)) = 0, NULL, TO_ARRAY(dotcom_duo_trials.product_rate_plan_category_general))
        ),
      ' ,'
      )                                                                           AS enabled_by_add_on_trial_product_at_event_time,
    COALESCE(
      namespace_subscription.enabled_by_oss_or_edu_rate_plan,
      installation_subscription.oss_or_edu_rate_plans
      )                                                                           AS enabled_by_oss_or_edu_rate_plan_at_event_time,

  FROM flattened_with_installation_id
  LEFT JOIN dim_namespace
    ON flattened_with_installation_id.enabled_by_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_app_release_major_minor
    ON regexp_substr(flattened_with_installation_id.gsc_instance_version,'(.*)[.]',1, 1, 'e') = dim_app_release_major_minor.major_minor_version
  LEFT JOIN wk_ping_installation_latest
    ON flattened_with_installation_id.dim_installation_id = wk_ping_installation_latest.dim_installation_id
  LEFT JOIN namespace_subscription
    ON flattened_with_installation_id.enabled_by_namespace_id = namespace_subscription.dim_namespace_id
    AND flattened_with_installation_id.behavior_at::DATE = namespace_subscription.date_actual::DATE
  LEFT JOIN add_on_namespace_sub_product
    ON flattened_with_installation_id.enabled_by_namespace_id = add_on_namespace_sub_product.dim_namespace_id
    AND flattened_with_installation_id.behavior_at::DATE = add_on_namespace_sub_product.date_actual::DATE
  LEFT JOIN installation_subscription
  ON flattened_with_installation_id.dim_installation_id = installation_subscription.dim_installation_id
    AND flattened_with_installation_id.behavior_at::DATE = installation_subscription.date_actual::DATE
  LEFT JOIN add_on_installation_sub_product
    ON flattened_with_installation_id.dim_installation_id = add_on_installation_sub_product.dim_installation_id
    AND flattened_with_installation_id.behavior_at::DATE = add_on_installation_sub_product.date_actual::DATE
  LEFT JOIN dotcom_duo_trials
    ON  flattened_with_installation_id.enabled_by_namespace_id = dotcom_duo_trials.dim_namespace_id
    AND flattened_with_installation_id.behavior_at >= dotcom_duo_trials.trial_start_date
    AND flattened_with_installation_id.behavior_at <= dotcom_duo_trials.latest_trial_end_date
  LEFT JOIN add_on_sm_trial
    ON flattened_with_installation_id.dim_installation_id = add_on_sm_trial.dim_installation_id
     AND flattened_with_installation_id.behavior_at::DATE = add_on_sm_trial.date_actual::DATE

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-08-30",
    updated_date="2025-02-24"
) }}

