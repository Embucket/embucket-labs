{{ config(
    
        materialized = "incremental",
        unique_key = "fct_behavior_unstructured_sk",
        full_refresh = true if flags.FULL_REFRESH and var('full_refresh_force', false) else false,
        on_schema_change = 'sync_all_columns'

) }}

{{ simple_cte([
    ('events', 'fct_behavior_unstructured_event'),
    ('dim_event', 'dim_behavior_event')
    ])
}}

, link_click AS (

    SELECT
      events.fct_behavior_unstructured_sk,
      events.behavior_at,
      events.dim_behavior_event_sk,
      events.dim_behavior_website_page_sk,
      events.dim_behavior_browser_sk,
      events.dim_behavior_operating_system_sk,
      events.gsc_pseudonymized_user_id,
      events.session_id,
      events.is_staging_event,
      events.link_click_target_url
    FROM events
    INNER JOIN dim_event
      ON events.dim_behavior_event_sk = dim_event.dim_behavior_event_sk
    WHERE dim_event.event_name = 'link_click'
      AND behavior_at >= DATEADD(MONTH, -25, CURRENT_DATE)

    {% if is_incremental() %}

      AND events.behavior_at > (SELECT max(behavior_at) FROM {{ this }})

    {% endif %}
)

{{ dbt_audit(
    cte_ref="link_click",
    created_by="@chrissharp",
    updated_by="@utkarsh060",
    created_date="2022-09-22",
    updated_date="2024-04-02"
) }}