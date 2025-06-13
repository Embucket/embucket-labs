{{ config(
    tags=["product"]
) }}


{{ simple_cte([
    ('prep_ci_build', 'prep_ci_build'),
    ('prep_ci_runner', 'prep_ci_runner'),
    ('prep_ci_pipeline', 'prep_ci_pipeline'),
    ('prep_ci_stage', 'prep_ci_stage'),
    ('prep_project', 'prep_project'),
    ('dim_namespace', 'dim_namespace'),
    ('prep_user', 'prep_user'),
    ('dim_date', 'dim_date')
]) }}

, joined AS (

    SELECT
      -- PRIMARY KEY
      prep_ci_build.dim_ci_build_id,

      -- FOREIGN KEYS
      IFNULL(prep_ci_runner.dim_ci_runner_id, -1)             AS dim_ci_runner_id,
      IFNULL(prep_ci_pipeline.dim_ci_pipeline_id, -1)         AS dim_ci_pipeline_id,
      IFNULL(prep_ci_stage.dim_ci_stage_id, -1)               AS dim_ci_stage_id,
      IFNULL(prep_project.dim_project_id, -1)                 AS dim_project_id,
      IFNULL(prep_user.dim_user_id, -1)                       AS dim_user_id,
      IFNULL(dim_date.date_id, -1)                            AS ci_build_created_date_id,
      IFNULL(dim_namespace.dim_namespace_id, -1)              AS dim_namespace_id,
      IFNULL(dim_namespace.ultimate_parent_namespace_id, -1)  AS ultimate_parent_namespace_id,
      prep_ci_build.dim_plan_id,

      -- ci_build metrics
      prep_ci_build.started_at                                AS ci_build_started_at,
      prep_ci_build.finished_at                               AS ci_build_finished_at,
      DATEDIFF('seconds',
               prep_ci_build.started_at,
               prep_ci_build.finished_at)                     AS ci_build_duration_in_s,

      -- ci_runner metrics
      CASE
        WHEN dim_namespace.namespace_is_internal = TRUE
          THEN TRUE
        WHEN prep_ci_runner.ci_runner_type = 1
          THEN TRUE
          ELSE FALSE
      END                                                     AS is_paid_by_gitlab,
      prep_ci_runner.public_projects_minutes_cost_factor,
      prep_ci_runner.private_projects_minutes_cost_factor

    FROM prep_ci_build
    LEFT JOIN prep_ci_runner
      ON prep_ci_build.dim_ci_runner_id = prep_ci_runner.dim_ci_runner_id
    LEFT JOIN prep_ci_stage
      ON prep_ci_build.dim_ci_stage_id = prep_ci_stage.dim_ci_stage_id
    LEFT JOIN prep_ci_pipeline
      ON prep_ci_stage.dim_ci_pipeline_id = prep_ci_pipeline.dim_ci_pipeline_id
    LEFT JOIN prep_project
      ON prep_ci_build.dim_project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace
      ON prep_ci_build.dim_namespace_id = dim_namespace.dim_namespace_id
    LEFT JOIN prep_user
      ON prep_ci_build.dim_user_id = prep_user.dim_user_id
    LEFT JOIN dim_date
      ON prep_ci_build.created_date_id = dim_date.date_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@ischweickartDD",
    created_date="2021-06-30",
    updated_date="2021-07-14"
) }}
