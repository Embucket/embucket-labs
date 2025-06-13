{{ config(

        materialized = "incremental",
        unique_key = "fct_behavior_unstructured_sk",
        full_refresh = only_force_full_refresh(),
        on_schema_change = 'sync_all_columns',
        tags=['product'],
        cluster_by = ['behavior_at::DATE']
,
snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ simple_cte([
    ('events', 'prep_snowplow_unnested_events_all'),
    ])
}}

, unstruct_event AS (

    SELECT
      event_id,
      behavior_at,
      event,
      dim_behavior_event_sk,
      is_staging_event,
      platform,
      gsc_pseudonymized_user_id,
      gsc_is_gitlab_team_member,
      user_snowplow_domain_id,
      clean_url_path,
      page_url_host,
      app_id,
      session_id,
      link_click_target_url,
      link_click_element_id,
      submit_form_id,
      change_form_id,
      change_form_type,
      change_form_element_id,
      focus_form_element_id,
      focus_form_node_name,
      dim_behavior_browser_sk,
      dim_behavior_operating_system_sk,
      dim_behavior_website_page_sk,
      dim_behavior_referrer_page_sk,
      environment
    FROM events
    WHERE event = 'unstruct'


    {% if is_incremental() %}

      AND behavior_at > (SELECT MAX({{ var('incremental_backfill_date', 'behavior_at') }}) FROM {{ this }})
      AND behavior_at <= (SELECT DATEADD(month, 1,  MAX({{ var('incremental_backfill_date', 'behavior_at') }}) )  FROM {{ this }})

    {% endif %}

), unstruct_event_with_dims AS (

    SELECT

      -- Primary Key
      {{ dbt_utils.generate_surrogate_key(['event_id','behavior_at']) }} AS fct_behavior_unstructured_sk,

      -- Natural Key
      unstruct_event.app_id,
      unstruct_event.event_id,
      unstruct_event.session_id,

      -- Surrogate Keys
      unstruct_event.dim_behavior_event_sk,
      unstruct_event.dim_behavior_website_page_sk,
      unstruct_event.dim_behavior_referrer_page_sk,
      unstruct_event.dim_behavior_browser_sk,
      unstruct_event.dim_behavior_operating_system_sk,

      --Time Attributes
      behavior_at,

      -- User Keys
      gsc_pseudonymized_user_id,
      user_snowplow_domain_id,

      -- Attributes
      is_staging_event,
      link_click_target_url,
      link_click_element_id,
      submit_form_id,
      change_form_id,
      change_form_type,
      change_form_element_id,
      focus_form_element_id,
      focus_form_node_name,
      gsc_is_gitlab_team_member
    FROM unstruct_event

)

{{ dbt_audit(
    cte_ref="unstruct_event_with_dims",
    created_by="@chrissharp",
    updated_by="@utkarsh060",
    created_date="2022-09-27",
    updated_date="2024-04-02"
) }}
