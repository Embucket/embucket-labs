{{ config(
    tags=["product"],
   snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_auto_devops_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, devops_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_auto_devops_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      devops_source.project_auto_devops_id                       AS dim_auto_devops_id,
      IFNULL(dim_project.dim_project_id, -1)                     AS dim_project_id,
      IFNULL(dim_project.ultimate_parent_namespace_id, -1)       AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)            AS dim_plan_id,
      dim_date.date_id                                           AS created_date_id,
      devops_source.created_at::TIMESTAMP                        AS created_at,
      devops_source.updated_at::TIMESTAMP                        AS updated_at
    FROM  devops_source
    LEFT JOIN dim_project
      ON  devops_source.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND  devops_source.created_at >= dim_namespace_plan_hist.valid_from
        AND  devops_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(devops_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-28",
    updated_date="2022-06-01"
) }}
