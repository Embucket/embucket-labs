{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_todo_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, gitlab_dotcom_todo_dedupe_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_todos_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      gitlab_dotcom_todo_dedupe_source.todo_id                          AS dim_todo_id,
      gitlab_dotcom_todo_dedupe_source.author_id                        AS author_id,
      IFNULL(dim_project.dim_project_id, -1)                            AS dim_project_id,
      IFNULL(dim_namespace_plan_hist.dim_namespace_id, -1)              AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                   AS dim_plan_id,
      dim_date.date_id                                                  AS created_date_id,
      gitlab_dotcom_todo_dedupe_source.created_at::TIMESTAMP            AS created_at,
      gitlab_dotcom_todo_dedupe_source.updated_at::TIMESTAMP            AS updated_at
    FROM gitlab_dotcom_todo_dedupe_source
    LEFT JOIN dim_project
      ON gitlab_dotcom_todo_dedupe_source.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_todo_dedupe_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_todo_dedupe_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(gitlab_dotcom_todo_dedupe_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-14",
    updated_date="2022-06-01"
) }}
