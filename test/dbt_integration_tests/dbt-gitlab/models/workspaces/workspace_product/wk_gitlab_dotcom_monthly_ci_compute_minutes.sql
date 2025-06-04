{{ config({
        "materialized": "table",
        "tags": ["product", "mnpi_exception"]
    })
}}

WITH pipeline_activity AS (

  SELECT
    DATE_TRUNC('month',ci_build_started_at)::DATE AS reporting_month,
    dim_namespace.ultimate_parent_namespace_id,
    dim_project_id, 
    dim_user_id,
    CASE 
      WHEN plan_name_modified LIKE '%trial%' THEN 'Trial' --plan_name_modified maps to current plan names
      WHEN plan_name_modified = 'ultimate' THEN 'Ultimate'
      WHEN plan_name_modified = 'premium' THEN 'Premium'
      WHEN plan_name_modified = 'default' THEN 'Free'
      ELSE plan_title
    END AS plan_title,
    CASE
      WHEN ci_runner_type_summary = 'shared' THEN 'Shared Runners'
      ELSE 'Self-Managed Runners'
    END AS runner_type,
    ci_runner_machine_type,
    cost_factor,
    COUNT(DISTINCT fct_ci_runner_activity.dim_ci_runner_id) as count_of_runners,
    COUNT(DISTINCT dim_ci_pipeline_id) as count_of_pipelines,
    SUM(ci_build_duration_in_s) / 60 AS ci_build_minutes
  FROM {{ ref('fct_ci_runner_activity') }} as fct_ci_runner_activity
  JOIN {{ ref ('dim_ci_runner') }} as dim_ci_runner
    ON fct_ci_runner_activity.dim_ci_runner_id = dim_ci_runner.dim_ci_runner_id
  JOIN {{ ref ('dim_namespace') }} as dim_namespace
    ON fct_ci_runner_activity.dim_namespace_id = dim_namespace.dim_namespace_id
  JOIN {{ ref('prep_gitlab_dotcom_plan') }} as prep_gitlab_dotcom_plan
    ON fct_ci_runner_activity.dim_plan_id = prep_gitlab_dotcom_plan.dim_plan_id
  WHERE ci_build_started_at >= '2020-01-01'
    AND ci_build_finished_at IS NOT NULL
    AND namespace_is_internal = FALSE
    AND namespace_creator_is_blocked = FALSE
  GROUP BY ALL

), purchased_minutes AS (

  SELECT
    date_trunc('month', effective_start_date) as reporting_month,
    namespace_id AS ultimate_parent_namespace_id,
    COUNT(DISTINCT dim_charge.dim_charge_id) as total_purchases
  FROM {{ref ('fct_charge') }}
  INNER JOIN {{ ref('dim_subscription') }}
    ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  LEFT JOIN {{ ref('dim_charge')}}
    ON fct_charge.dim_charge_id = dim_charge.dim_charge_id
  WHERE effective_end_date_id > effective_start_date_id
    AND rate_plan_name IN ('1,000 CI Minutes','1,000 Compute Minutes')
  GROUP BY ALL

), final AS (

SELECT 
  {{ dbt_utils.generate_surrogate_key(['pipeline_activity.reporting_month', 'pipeline_activity.ultimate_parent_namespace_id']) }} as namespace_reporting_month_pk,
  pipeline_activity.reporting_month, 
  pipeline_activity.ultimate_parent_namespace_id, 
  dim_project_id,
  dim_user_id, 
  plan_title, 
  runner_type,
  ci_runner_machine_type,
  count_of_runners,
  count_of_pipelines,
  ci_build_minutes,
  CASE WHEN cost_factor = 0 THEN ci_build_minutes
       ELSE ci_build_minutes * cost_factor
       END AS compute_minutes_with_cost_factor,
  CASE 
    WHEN plan_title = 'Free' THEN 400
    WHEN plan_title = 'Premium' THEN 10000
    WHEN plan_title = 'Ultimate' THEN 50000
    ELSE 400
    END as included_compute_credits,
  total_purchases
  FROM pipeline_activity
  LEFT JOIN purchased_minutes
    ON pipeline_activity.ultimate_parent_namespace_id = purchased_minutes.ultimate_parent_namespace_id
    AND pipeline_activity.reporting_month = purchased_minutes.reporting_month
  WHERE pipeline_activity.reporting_month < date_trunc('month', current_date())
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@nhervas",
    updated_by="@nhervas",
    created_date="2024-01-02",
    updated_date="2024-05-22"
) }}

