{{ config(
        materialized = "incremental",
        unique_key = "behavior_structured_event_pk",
        tags=['product'],
        on_schema_change = "sync_all_columns",
        snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{
    simple_cte([
    ('fct_behavior_structured_event', 'fct_behavior_structured_event'),
    ('namespaces_hist', 'gitlab_dotcom_namespace_lineage_scd'),
    ('dim_behavior_event', 'dim_behavior_event')
    ])
}}

, final AS (

    SELECT

      -- Primary Key
      fct_behavior_structured_event.behavior_structured_event_pk,

      -- Foreign Keys
      fct_behavior_structured_event.dim_behavior_website_page_sk,
      fct_behavior_structured_event.dim_behavior_browser_sk,
      fct_behavior_structured_event.dim_behavior_operating_system_sk,
      fct_behavior_structured_event.dim_namespace_id,
      namespaces_hist.ultimate_parent_id                              AS ultimate_parent_namespace_id,
      fct_behavior_structured_event.dim_project_id,
      fct_behavior_structured_event.dim_behavior_event_sk,

      -- Time Attributes
      fct_behavior_structured_event.dvce_created_tstamp,
      fct_behavior_structured_event.behavior_at,

      -- Degenerate Dimensions (Event Attributes)
      fct_behavior_structured_event.tracker_version,
      fct_behavior_structured_event.session_index,
      fct_behavior_structured_event.app_id,
      fct_behavior_structured_event.session_id,
      fct_behavior_structured_event.user_snowplow_domain_id,
      fct_behavior_structured_event.contexts,
      fct_behavior_structured_event.is_staging_event,
      dim_behavior_event.event_action,


      -- Degenerate Dimensions (Gitlab Standard Context Attributes)
      fct_behavior_structured_event.gsc_google_analytics_client_id,
      fct_behavior_structured_event.gsc_pseudonymized_user_id,
      fct_behavior_structured_event.gsc_environment,
      fct_behavior_structured_event.gsc_extra,
      fct_behavior_structured_event.gsc_plan,
      fct_behavior_structured_event.gsc_source,
      fct_behavior_structured_event.gsc_is_gitlab_team_member

    FROM fct_behavior_structured_event
    INNER JOIN dim_behavior_event
      ON fct_behavior_structured_event.dim_behavior_event_sk = dim_behavior_event.dim_behavior_event_sk
    LEFT JOIN namespaces_hist ON namespaces_hist.namespace_id = fct_behavior_structured_event.dim_namespace_id
      AND fct_behavior_structured_event.behavior_at BETWEEN namespaces_hist.lineage_valid_from AND namespaces_hist.lineage_valid_to
    WHERE dim_behavior_event.event_action IN (
    'g_analytics_valuestream',
    'action_active_users_project_repo',
    'push_package',
    'ci_templates_unique',
    'p_terraform_state_api_unique_users',
    'i_search_paid'
    )

    {% if is_incremental() %}

    AND behavior_at > (SELECT MAX(behavior_at) FROM {{this}})

    {% endif %}


)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@utkarsh060",
    created_date="2022-09-01",
    updated_date="2024-04-04"
) }}
