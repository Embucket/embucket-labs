{{
  config(
    materialized='incremental',
    unique_key = "zoekt_rollout_category_report_date_dim_namespace_id",
    full_refresh = false,
    on_schema_change = "sync_all_columns",
    tags=["product"]
  )
}}

{{ simple_cte([
   ('dim_namespace', 'dim_namespace'),
   ('prep_gitlab_dotcom_plan', 'prep_gitlab_dotcom_plan'),
   ('gitlab_dotcom_elasticsearch_indexed_namespaces_source', 'gitlab_dotcom_elasticsearch_indexed_namespaces_source'),
   ('gitlab_dotcom_zoekt_enabled_namespaces', 'gitlab_dotcom_zoekt_enabled_namespaces'),
   ('gitlab_dotcom_zoekt_indices', 'gitlab_dotcom_zoekt_indices')
]) }}

, zoekt_rollout_category AS (

  SELECT DISTINCT
    CURRENT_DATE()                                                              AS report_date,
    dim_namespace.dim_namespace_id,
    dim_namespace.ultimate_parent_namespace_id,
    dim_namespace.created_at::DATE                                              AS namespace_created_date,
    dim_namespace.namespace_type,
    dim_namespace.visibility_level,
    dim_namespace.namespace_creator_is_blocked,
    dim_namespace.namespace_is_internal,
    dim_namespace.namespace_is_ultimate_parent,
    dim_namespace.gitlab_plan_is_paid,
    prep_gitlab_dotcom_plan.plan_name_modified                                  AS current_gitlab_plan_title,
    zoekt_enabled_namespaces.search,
    IFF(elasticsearch_indexed_namespaces.namespace_id IS NOT NULL, TRUE, FALSE) AS is_elasticsearch_indexed_namespaces_record,
    IFF(zoekt_enabled_namespaces.root_namespace_id IS NOT NULL, TRUE, FALSE)    AS is_zoekt_enabled_namespaces_record,
    IFF(zoekt_enabled_namespaces_indices.namespace_id IS NOT NULL, TRUE, FALSE) AS is_zoekt_indices_record,
    CASE
      WHEN is_zoekt_enabled_namespaces_record = FALSE THEN 'not indexed'
      WHEN is_zoekt_enabled_namespaces_record = TRUE AND is_zoekt_indices_record = FALSE AND zoekt_enabled_namespaces.search IS NULL THEN 'not indexed'
      WHEN is_zoekt_enabled_namespaces_record = TRUE AND is_zoekt_indices_record = FALSE AND zoekt_enabled_namespaces.search = TRUE THEN 'not indexed - search enabled'
      WHEN is_zoekt_enabled_namespaces_record = TRUE AND is_zoekt_indices_record = FALSE AND zoekt_enabled_namespaces.search = FALSE THEN 'not indexed - search disabled'
      WHEN is_zoekt_enabled_namespaces_record = TRUE AND is_zoekt_indices_record = TRUE AND zoekt_enabled_namespaces.search = FALSE THEN 'indexed - search disabled'
      WHEN is_zoekt_enabled_namespaces_record = TRUE AND is_zoekt_indices_record = TRUE AND zoekt_enabled_namespaces.search = TRUE THEN 'indexed - search enabled'
      ELSE 'error'
    END                                                                         AS zoekt_rollout_categories
  FROM dim_namespace
  INNER JOIN prep_gitlab_dotcom_plan AS prep_gitlab_dotcom_plan -- to get clean plan name
    ON dim_namespace.gitlab_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  INNER JOIN gitlab_dotcom_elasticsearch_indexed_namespaces_source AS elasticsearch_indexed_namespaces -- all advanced search namespaces for .com
    ON dim_namespace.dim_namespace_id = elasticsearch_indexed_namespaces.namespace_id
  LEFT JOIN gitlab_dotcom_zoekt_enabled_namespaces AS zoekt_enabled_namespaces -- zoekt enabled namespaces
    ON zoekt_enabled_namespaces.root_namespace_id = elasticsearch_indexed_namespaces.namespace_id
  LEFT JOIN gitlab_dotcom_zoekt_indices AS zoekt_enabled_namespaces_indices
    ON zoekt_enabled_namespaces_indices.namespace_id = zoekt_enabled_namespaces.root_namespace_id

), final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(["report_date", "dim_namespace_id", "zoekt_rollout_categories"]) }} AS zoekt_rollout_category_report_date_dim_namespace_id,
    report_date,
    dim_namespace_id,
    ultimate_parent_namespace_id,
    namespace_created_date,
    namespace_type,
    visibility_level,
    namespace_creator_is_blocked,
    namespace_is_internal,
    namespace_is_ultimate_parent,
    gitlab_plan_is_paid,
    current_gitlab_plan_title,
    search,
    zoekt_rollout_categories,
  FROM zoekt_rollout_category
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@utkarsh060",
    updated_by="@utkarsh060",
    created_date="2024-05-07",
    updated_date="2024-05-09"
) }}
