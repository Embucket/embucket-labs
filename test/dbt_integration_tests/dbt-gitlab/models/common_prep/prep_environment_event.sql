{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_environment_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, environment_event AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_environments_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      environment_event.environment_id                              AS dim_environment_id,
      IFNULL(environment_event.project_id, -1)                      AS dim_project_id,
      IFNULL(dim_project.ultimate_parent_namespace_id, -1)          AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)               AS dim_plan_id,
      environment_event.created_at::TIMESTAMP                       AS created_at,
      environment_event.updated_at::TIMESTAMP                       AS updated_at,
      dim_date.date_id                                              AS created_date_id
    FROM environment_event
    LEFT JOIN dim_project ON environment_event.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND environment_event.created_at >= dim_namespace_plan_hist.valid_from
        AND environment_event.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(environment_event.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-03-16",
    updated_date="2022-03-16"
) }}
