{{ config({
        "materialized": "table",
        "unique_key": "event_reporting_month_pk",
        "tags": ["product", "mnpi_exception"],
        "on_schema_change":"sync_all_columns"
    })
}}

WITH structured_events AS (

    SELECT 
        DATE_TRUNC('month', behavior_date)                  AS reporting_month, 
        event_category,
        ultimate_parent_namespace_id,
        dim_project_id                                      AS gsc_project_id,
        gsc_pseudonymized_user_id,
        plan_name_modified,
        CASE 
        WHEN event_action IN ('pull_package', 'push_package', 'delete_package', 'npm_request_forward', 'list_package', 'register_package',
        'docker_container_retention_and_expiration_policies', 'delete_repository', 'delete_tag', 'delete_tag_bulk', 'list_repositories', 
        'list_tags', 'pull_manifest', 'pull_manifest_from_cache', 'pull_blob', 'pull_blob_from_cache', 'pypi_request_forward', 'push_tag')
          THEN event_action
        WHEN event_action = 'delete_package_file' 
          OR event_action = 'delete_package_files'
          THEN 'delete_package_files'
        WHEN event_action = 'copy_pypi_request_forward'
          THEN 'pypi_request_forward'
        WHEN event_action IN ('copy_conan_command', 'copy_conan_setup_xml')
            AND event_label = 'code_instruction'
        THEN 'conan_code_snippet'
        WHEN event_action = 'confirm_delete'
          THEN 'container_registry_ui_delete_actions'
        WHEN event_action IN ('bulk_registry_tag_delete', 'registry_repository_delete', 'registry_tag_delete')
          THEN 'ui_delete'
        WHEN event_category = 'container_registry:notification'
          THEN event_action
        WHEN event_category = 'projects:registry:repositories:index'
          AND event_label = 'quickstart_dropdown'
          THEN 'quick_setup'
        END                                                 AS metric,
        COUNT(DISTINCT behavior_structured_event_pk)        AS total_events
    FROM {{ ref('mart_behavior_structured_event') }}
    WHERE metric IS NOT NULL
    --- filter out any internal namespaces
      AND (namespace_is_internal = FALSE OR ultimate_parent_namespace_id IS NULL)
  {{ dbt_utils.group_by(n=7) }}

), pageviews AS (

    SELECT 
        date_trunc('month', behavior_at)                    AS reporting_month, 
        NULL as event_category,
        plan_name_modified,
        ultimate_parent_namespace_id,
        gsc_project_id,
        gsc_pseudonymized_user_id,
        CASE 
          WHEN page_url_path LIKE '%container_regist%'
            THEN 'page_view_container_registry'
          WHEN page_url_path LIKE '%package%'
            THEN 'page_view_package'
          WHEN page_url_path LIKE '%/groups%'
            AND page_url_path LIKE '%container_regist%'
            THEN 'page_view_container_registry_group'
          WHEN page_url_path LIKE '%/groups%'
            AND page_url_path LIKE '%package%'
            THEN 'page_view_package_group'
          WHEN page_url_path LIKE '%/google_cloud/artifact_registry'
            THEN 'page_view_google_artifact_registry'
        END                                                 AS metric, 
        COUNT(DISTINCT fct_behavior_website_page_view_sk)   AS total_events
    FROM {{ ref('fct_behavior_website_page_view') }} as page_view
    LEFT JOIN {{ ref('dim_plan')}} as plan 
      ON plan.plan_name = page_view.gsc_plan
    LEFT JOIN {{ ref('dim_namespace')}} as namespaces
      ON namespaces.dim_namespace_id = page_view.gsc_namespace_id
    WHERE metric IS NOT NULL
    --- filter out any namespaces that are internal and include any data from any records with no namespace
      AND (namespace_is_internal = FALSE OR ultimate_parent_namespace_id IS NULL)
  {{ dbt_utils.group_by(n=7) }}

), final AS (

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['reporting_month', 'ultimate_parent_namespace_id']) }} AS event_reporting_month_pk,
    reporting_month, 
    event_category, 
    metric,
    plan_name_modified,
    ultimate_parent_namespace_id,
    gsc_project_id,
    gsc_pseudonymized_user_id,
    total_events 
  FROM structured_events

UNION ALL 

  SELECT 
    {{ dbt_utils.generate_surrogate_key(['reporting_month', 'ultimate_parent_namespace_id']) }} AS event_reporting_month_pk,
    reporting_month, 
    event_category, 
    metric,
    plan_name_modified,
    ultimate_parent_namespace_id,
    gsc_project_id,
    gsc_pseudonymized_user_id,
    total_events 
  FROM pageviews
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@nhervas",
    updated_by="@nhervas",
    created_date="2023-09-05",
    updated_date="2024-03-28"
) }}
