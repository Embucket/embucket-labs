{{ config(

    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='behavior_structured_event_pk',
    tags=['product'],
    full_refresh= only_force_full_refresh(),
    on_schema_change='sync_all_columns',
    cluster_by=['behavior_at::DATE','event_action'],
    automatic_clustering='true'
  )

}}

{{ simple_cte([
    ('event', 'dim_behavior_event'),
    ('namespace', 'dim_namespace'),
    ('project', 'dim_project'),
    ('operating_system', 'dim_behavior_operating_system'),
    ('browser','dim_behavior_browser'),
    ('plan','dim_plan'),
    ('dates', 'dim_date')
]) }},

structured_behavior AS (

  SELECT
    behavior_structured_event_pk,
    behavior_at,
    app_id,
    user_snowplow_domain_id,
    contexts,
    page_url_path,
    page_url_fragment,
    gsc_google_analytics_client_id,
    gsc_pseudonymized_user_id,
    gsc_extra,
    gsc_plan,
    gsc_source,
    gsc_is_gitlab_team_member,
    gsc_feature_enabled_by_namespace_ids,
    event_value,
    session_index,
    session_id,
    has_gitlab_service_ping_context,
    has_gitlab_experiment_context,
    has_customer_standard_context,
    dim_behavior_referrer_page_sk,
    dim_behavior_event_sk,
    dim_namespace_id,
    dim_project_id,
    dim_behavior_operating_system_sk,
    dim_behavior_browser_sk,
    dim_plan_sk,
    user_region,
    user_region_name,
    user_city,
    user_country,
    user_timezone_name,
    dim_instance_id,
    host_name,
    gsc_instance_version,
    delivery_type,
    gitlab_global_user_id,
    gsc_correlation_id,
    interface,
    client_type,
    client_name,
    client_version,
    feature_category
  FROM {{ ref('fct_behavior_structured_event') }}
  WHERE is_staging_event = FALSE
  {% if is_incremental() %}

      AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(MONTH, 1, MAX({{ var('incremental_backfill_date', 'behavior_at') }})) FROM {{ this }})

  {% else %}
  -- This will cover the first creation of the table or a full refresh and requires that the table be backfilled
  AND behavior_at > DATEADD('day', -30 ,CURRENT_DATE())

  {% endif %}

),

report AS (
  SELECT
    structured_behavior.behavior_structured_event_pk,
    structured_behavior.behavior_at,
    dates.date_actual AS behavior_date,
    structured_behavior.app_id,
    structured_behavior.user_snowplow_domain_id,
    structured_behavior.contexts,
    structured_behavior.page_url_path,
    structured_behavior.page_url_fragment,
    structured_behavior.gsc_google_analytics_client_id,
    structured_behavior.gsc_pseudonymized_user_id,
    structured_behavior.gsc_extra,
    structured_behavior.gsc_plan,
    structured_behavior.gsc_source,
    structured_behavior.gsc_is_gitlab_team_member,
    structured_behavior.gsc_feature_enabled_by_namespace_ids,
    structured_behavior.event_value,
    structured_behavior.session_index,
    structured_behavior.session_id,
    structured_behavior.has_gitlab_service_ping_context,
    structured_behavior.has_gitlab_experiment_context,
    structured_behavior.has_customer_standard_context,
    structured_behavior.user_region,
    structured_behavior.user_region_name,
    structured_behavior.user_city,
    structured_behavior.user_country,
    structured_behavior.user_timezone_name,
    structured_behavior.dim_instance_id,
    structured_behavior.host_name,
    structured_behavior.gsc_instance_version,
    structured_behavior.gsc_correlation_id,
    structured_behavior.delivery_type,
    structured_behavior.gitlab_global_user_id,
    structured_behavior.interface,
    structured_behavior.client_name,
    structured_behavior.client_type,
    structured_behavior.client_version,
    structured_behavior.feature_category,
    event.event_category,
    event.event_action,
    event.event_label,
    event.event_property,
    namespace.dim_namespace_id,
    namespace.ultimate_parent_namespace_id,
    namespace.namespace_is_internal,
    namespace.namespace_is_ultimate_parent,
    namespace.namespace_type,
    namespace.visibility_level,
    project.dim_project_id,
    operating_system.device_type,
    operating_system.is_device_mobile,
    browser.browser_name,
    browser.dim_behavior_browser_sk,
    plan.dim_plan_id,
    plan.plan_id_modified,
    plan.plan_name,
    plan.plan_name_modified,
    structured_behavior.dim_behavior_referrer_page_sk,
  FROM structured_behavior
  LEFT JOIN event
    ON structured_behavior.dim_behavior_event_sk = event.dim_behavior_event_sk
  LEFT JOIN namespace
    ON structured_behavior.dim_namespace_id = namespace.dim_namespace_id
  LEFT JOIN project
    ON structured_behavior.dim_project_id = project.dim_project_id
  LEFT JOIN operating_system
    ON structured_behavior.dim_behavior_operating_system_sk = operating_system.dim_behavior_operating_system_sk
  LEFT JOIN browser
    ON structured_behavior.dim_behavior_browser_sk = browser.dim_behavior_browser_sk
  LEFT JOIN plan
  ON structured_behavior.dim_plan_sk = plan.dim_plan_sk
  LEFT JOIN dates
    ON{{ get_date_id('structured_behavior.behavior_at') }} = dates.date_id
)

{{ dbt_audit(
    cte_ref="report",
    created_by="@pempey",
    updated_by="@michellecooper",
    created_date="2023-02-22",
    updated_date="2024-12-04"
) }}
