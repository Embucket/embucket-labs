{{ config({
    "materialized": "incremental",
    "unique_key": "behavior_structured_event_pk",
    "tags": ["product","mnpi_exception"],
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH filtered_snowplow_events AS (

  SELECT
    CASE
      WHEN
        event_label LIKE 'group_dropdown_frequent_items_list_item_%'
        THEN 'group_dropdown_frequent_items_list_item'
      WHEN
        event_label LIKE 'groups_dropdown_frequent_items_list_item_%'
        THEN 'group_dropdown_frequent_items_list_item'
      WHEN
        event_label LIKE 'project_dropdown_frequent_items_list_item_%'
        THEN 'project_dropdown_frequent_items_list_item'
      WHEN
        event_label LIKE 'projects_dropdown_frequent_items_list_item_%'
        THEN 'project_dropdown_frequent_items_list_item'
      ELSE event_label
    END AS event_label,
{{ dbt_utils.star(from=ref('mart_behavior_structured_event'), except=["EVENT_LABEL","CREATED_BY", "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM {{ ref('mart_behavior_structured_event') }}
  WHERE behavior_at >= '2021-10-01'
    AND app_id IN ('gitlab', 'gitlab_customers')
    AND -- the conditions below represent the process of adding nav events until the naming convention improved to precede event_labels with nav_ hence the OR
    (
      (
        (
          (
            event_label IN (
              'main_navigation',
              'profile_dropdown',
              'groups_side_navigation',
              'kubernetes_sections_tabs',
              'hamburger_menu',
              'menu_view_all_projects',
              'menu_view_all_groups',
              'menu_milestones',
              'menu_snippets',
              'menu_environments',
              'menu_operations',
              'menu_security',
              'new_dropdown',
              'plus_menu_dropdown',
              'main_navigation',
              'profile_dropdown'
            )
          )
          OR (
            event_action IN (
              'click_whats_new_drawer',
              'click_forum'
            )
          )
          OR (
            event_label IN (
              'Menu',
              'groups_dropdown',
              'projects_dropdown'
            )
            AND event_action = 'click_dropdown'
          )
          OR
          (
            event_action IN ('click_menu', 'click_menu_item')
            AND
            (event_category LIKE 'dashboard%' OR event_category LIKE 'root%' OR event_category LIKE 'projects%')
          )
          OR
          (
            event_action = 'render' AND event_label = 'user_side_navigation'
          )
        )
        OR
        event_label LIKE ANY ('groups_dropdown_%', 'project_dropdown_%', 'group_dropdown_%', 'projects_dropdown_%')
        OR
        (
          event_label IN ('packages_registry', 'container_registry', 'infrastructure_registry', 'kubernetes', 'terraform')
          AND
          event_action = 'click_menu_item'
          AND
          event_category LIKE 'groups%'
        )
        OR
        event_action = 'click_pinned_menu_item'
      )
      OR
      event_property LIKE 'nav%' -- this condition should catch all new nav events. Preceding block of logic allows the historical events to also be added.
    )
  {% if is_incremental() %}

    AND  behavior_at > (SELECT MAX(behavior_at) FROM {{ this }})

  {% endif %}
)

{{ dbt_audit(
    cte_ref="filtered_snowplow_events",
    created_by="@mdrussell",
    updated_by="@matthewpetersen",
    created_date="2023-06-15",
    updated_date="2023-06-21"
) }}
