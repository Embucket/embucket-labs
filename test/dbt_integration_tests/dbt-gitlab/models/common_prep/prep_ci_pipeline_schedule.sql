{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_pipeline_schedule_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project')
]) }}

, pipeline_schedule AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_pipeline_schedules_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      ci_pipeline_schedule_id                                       AS dim_ci_pipeline_schedule_id,
      pipeline_schedule.owner_id                                    AS dim_user_id,
      pipeline_schedule.created_at,
      pipeline_schedule.updated_at,
      dim_date.date_id                                              AS created_date_id,
      IFNULL(dim_project.dim_project_id, -1)                        AS dim_project_id,
      IFNULL(dim_project.ultimate_parent_namespace_id, -1)          AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)               AS dim_plan_id
    FROM pipeline_schedule
    LEFT JOIN dim_project
      ON pipeline_schedule.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist
      ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND  pipeline_schedule.created_at >= dim_namespace_plan_hist.valid_from
      AND  pipeline_schedule.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(pipeline_schedule.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-04-01",
    updated_date="2022-06-01"
) }}
