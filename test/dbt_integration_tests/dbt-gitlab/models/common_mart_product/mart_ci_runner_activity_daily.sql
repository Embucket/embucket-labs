

{{ simple_cte([
   ('ci_runner_activity', 'fct_ci_runner_activity'),
   ('dim_namespace', 'dim_namespace'),
   ('dim_project', 'dim_project'),
   ('dim_date', 'dim_date')
]) }}

, ci_runner_activity_daily AS (

   SELECT
     dim_date.date_day                                                 AS report_date,
     ci_runner_activity.dim_project_id,
     ci_runner_activity.dim_namespace_id,
     ci_runner_activity.dim_ci_runner_id,
     ci_runner_activity.dim_ci_pipeline_id,
     ci_runner_activity.dim_ci_stage_id,
     ci_runner_activity.is_paid_by_gitlab,
     ci_runner_activity.public_projects_minutes_cost_factor,
     ci_runner_activity.private_projects_minutes_cost_factor,
     COUNT(DISTINCT ci_runner_activity.dim_ci_build_id)                AS job_count,
     SUM(ci_runner_activity.ci_build_duration_in_s)                    AS ci_build_duration_in_s
   FROM ci_runner_activity
   INNER JOIN dim_date
      ON TO_DATE(ci_runner_activity.ci_build_started_at) = dim_date.date_day
   WHERE ci_runner_activity.ci_build_finished_at IS NOT NULL
   {{ dbt_utils.group_by(n=9) }}

), joined AS (

   SELECT
     ci_runner_activity_daily.report_date,
     dim_project.dim_project_id,
     dim_namespace.dim_namespace_id,
     dim_namespace.ultimate_parent_namespace_id,
     dim_namespace.gitlab_plan_id                                      AS dim_plan_id,
     ci_runner_activity_daily.dim_ci_runner_id,
     ci_runner_activity_daily.dim_ci_pipeline_id,
     ci_runner_activity_daily.dim_ci_stage_id,


     -- CI RUNNER METRICS
     ci_runner_activity_daily.job_count,
     ci_runner_activity_daily.ci_build_duration_in_s,
     ci_runner_activity_daily.public_projects_minutes_cost_factor,
     ci_runner_activity_daily.private_projects_minutes_cost_factor,

     -- CI RUNNER ACTIVITY METADATA
     ci_runner_activity_daily.is_paid_by_gitlab,
     dim_project.visibility_level                                      AS project_visibility_level,
     dim_project.project_path,
     dim_namespace.namespace_is_internal,
     dim_namespace.gitlab_plan_title                                   AS ultimate_parent_plan_title

   FROM ci_runner_activity_daily
   INNER JOIN dim_project
     ON ci_runner_activity_daily.dim_project_id = dim_project.dim_project_id
   INNER JOIN dim_namespace
     ON ci_runner_activity_daily.dim_namespace_id = dim_namespace.dim_namespace_id

)

{{ dbt_audit(
   cte_ref="joined",
   created_by="@ischweickartDD",
   updated_by="@davis_townsend",
   created_date="2021-07-30",
   updated_date="2021-11-09"
) }}
