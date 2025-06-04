{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('gitlab_dotcom_labels_source', 'gitlab_dotcom_labels_source'),
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, renamed AS (

    SELECT
      gitlab_dotcom_labels_source.label_id                              AS dim_label_id,
      -- FOREIGN KEYS
      gitlab_dotcom_labels_source.project_id                            AS dim_project_id,
      IFNULL(gitlab_dotcom_labels_source.group_id,
        dim_project.ultimate_parent_namespace_id)                       AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                   AS dim_plan_id,
      --
      gitlab_dotcom_labels_source.group_id                              AS dim_namespace_id,
      gitlab_dotcom_labels_source.label_title,
      gitlab_dotcom_labels_source.label_type,
      gitlab_dotcom_labels_source.created_at,
      dim_date.date_id                                                  AS created_date_id
    FROM gitlab_dotcom_labels_source
    LEFT JOIN dim_project
      ON gitlab_dotcom_labels_source.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_labels_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_labels_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date
      ON TO_DATE(gitlab_dotcom_labels_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@chrissharp",
    created_date="2021-08-04",
    updated_date="2022-06-01"
) }}
