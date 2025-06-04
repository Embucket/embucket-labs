{{ config({
        "materialized": "table",
        "tags": ["product", "mnpi_exception"]
    })
}}

WITH page_views AS (

    SELECT 
      date_trunc('month', behavior_at)                  AS reporting_month, 
      plan_name_modified,
      dim_namespace.ultimate_parent_namespace_id        AS ultimate_parent_namespace_id,
      gsc_project_id,
      CASE 
        WHEN page_url_path LIKE '%ci/editor%' 
          THEN 'pipeline_editor_pageview'
      END                                               AS metric, 
      COUNT(DISTINCT fct_behavior_website_page_view_sk) AS total_events,
      COUNT(DISTINCT gsc_pseudonymized_user_id) as total_users
    FROM {{ ref('fct_behavior_website_page_view') }}
    LEFT JOIN {{ ref('dim_plan') }}
      ON dim_plan.plan_name = fct_behavior_website_page_view.gsc_plan
    LEFT JOIN {{ref ('dim_namespace')}}
      ON dim_namespace.dim_namespace_id = fct_behavior_website_page_view.gsc_namespace_id
    WHERE metric IS NOT NULL
        --- filter out any namespaces that are internal and include any data from any records with no namespace
      AND (namespace_is_internal = FALSE OR gsc_namespace_id IS NULL)
    GROUP BY ALL

), structured_events AS (

    SELECT 
      date_trunc('month', behavior_at)                  AS reporting_month, 
      plan_name_modified,
      ultimate_parent_namespace_id,
      dim_project_id                                    AS gsc_project_id,
      CASE
        WHEN event_label = 'pipelines_filtered_search'
          AND event_action = 'click_filtered_search'
          THEN 'click_search'
        WHEN event_label = 'pipeline_id_iid_listbox'
          THEN 'pipeline_iid_chart'
        WHEN event_label = 'pipelines_filter_tabs'
          AND event_action = 'click_filter_tabs'
          THEN 'pipeline_table_tab_clicks'
        WHEN event_label = 'pipelines_table_component'
          AND event_action in( 'click_retry_button', 'click_manual_actions', 'click_artifacts_dropdown', 'click_cancel_button')
          THEN 'pipeline_table_action_buttons'
        WHEN event_label = 'pipeline_editor'
          AND event_action = 'browse_catalog'
          AND event_category = 'projects:ci:pipeline_editor:show'
          THEN 'browser_catalog_clicks'
        WHEN event_action = 'unique_users_visiting_ci_catalog'
          AND event_category = 'InternalEventTracking'
          THEN 'users_visiting_ci_catalog'
      END                                               AS metric,
      COUNT(DISTINCT gsc_pseudonymized_user_id)         AS total_users,
      COUNT(DISTINCT behavior_structured_event_pk)      AS total_events
    FROM {{ ref('mart_behavior_structured_event') }}
    WHERE metric IS NOT NULL
    --- filter out any namespaces that are internal and include any data from any records with no namespace
      AND (namespace_is_internal = FALSE OR ultimate_parent_namespace_id IS NULL)
    GROUP BY ALL

), final AS (

    SELECT 
      {{ dbt_utils.generate_surrogate_key(['reporting_month', 'ultimate_parent_namespace_id']) }} AS event_reporting_month_pk,
      reporting_month, 
      plan_name_modified,
      ultimate_parent_namespace_id, 
      gsc_project_id,
      metric,
      total_events,
      total_users
    FROM page_views

    UNION ALL 

    SELECT 
      {{ dbt_utils.generate_surrogate_key(['reporting_month', 'ultimate_parent_namespace_id']) }} AS event_reporting_month_pk,
      reporting_month, 
      plan_name_modified,
      ultimate_parent_namespace_id, 
      gsc_project_id,
      metric,
      total_events,
      total_users
    FROM structured_events

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@nhervas",
    updated_by="@nhervas",
    created_date="2024-02-15",
    updated_date="2024-04-17"
) }}
