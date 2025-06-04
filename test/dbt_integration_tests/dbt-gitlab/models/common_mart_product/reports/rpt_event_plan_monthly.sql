{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('mart_event_valid', 'mart_event_valid'),
    ('dim_plan', 'dim_plan')
    ])
}},

mart_with_date_range AS (

  SELECT
    mart_event_valid.dim_ultimate_parent_namespace_id,
    mart_event_valid.event_calendar_month,
    mart_event_valid.event_calendar_quarter,
    mart_event_valid.event_calendar_year,
    mart_event_valid.event_name,
    mart_event_valid.stage_name,
    mart_event_valid.section_name,
    mart_event_valid.group_name,
    mart_event_valid.is_smau,
    mart_event_valid.is_gmau,
    mart_event_valid.is_umau,
    mart_event_valid.dim_user_id,
    mart_event_valid.plan_id_at_event_date,
    mart_event_valid.event_created_at,
    dim_date.last_day_of_month       AS last_day_of_month,
    dim_date.last_day_of_quarter     AS last_day_of_quarter,
    dim_date.last_day_of_fiscal_year AS last_day_of_fiscal_year
  FROM mart_event_valid
  LEFT JOIN dim_date
    ON mart_event_valid.event_date = dim_date.date_actual
  WHERE mart_event_valid.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

),

plan_id_by_month AS (

  SELECT
    mart_with_date_range.dim_ultimate_parent_namespace_id,
    mart_with_date_range.event_calendar_month,
    mart_with_date_range.plan_id_at_event_date,
    dim_plan.plan_name_modified AS plan_name_at_event_date -- plan_name_modified maps to current plan names
  FROM mart_with_date_range
  INNER JOIN dim_plan
    ON mart_with_date_range.plan_id_at_event_date = dim_plan.dim_plan_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY mart_with_date_range.dim_ultimate_parent_namespace_id, mart_with_date_range.event_calendar_month
    ORDER BY mart_with_date_range.event_created_at DESC) = 1

),

mart_usage_event_plan_monthly AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['mart_with_date_range.event_calendar_month', 'plan_id_by_month.plan_id_at_event_date', 'mart_with_date_range.event_name']) }} AS event_plan_monthly_id,
    mart_with_date_range.event_calendar_month,
    mart_with_date_range.event_calendar_quarter,
    mart_with_date_range.event_calendar_year,
    plan_id_by_month.plan_id_at_event_date                            AS plan_id_at_event_month,
    plan_id_by_month.plan_name_at_event_date                          AS plan_name_at_event_month,
    mart_with_date_range.event_name,
    mart_with_date_range.stage_name,
    mart_with_date_range.section_name,
    mart_with_date_range.group_name,
    mart_with_date_range.is_smau,
    mart_with_date_range.is_gmau,
    mart_with_date_range.is_umau,
    COUNT(*)                                                          AS event_count,
    COUNT(DISTINCT plan_id_by_month.dim_ultimate_parent_namespace_id) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT mart_with_date_range.dim_user_id)                  AS user_count
  FROM mart_with_date_range
  LEFT JOIN plan_id_by_month
    ON mart_with_date_range.dim_ultimate_parent_namespace_id IS NOT DISTINCT FROM plan_id_by_month.dim_ultimate_parent_namespace_id     -- treats NULLs as Values
      AND mart_with_date_range.event_calendar_month = plan_id_by_month.event_calendar_month
  WHERE mart_with_date_range.event_calendar_month < DATE_TRUNC('month', CURRENT_DATE)
  {{ dbt_utils.group_by(n=13) }}
  ORDER BY mart_with_date_range.event_calendar_month DESC, plan_id_by_month.plan_id_at_event_date DESC

)

{{ dbt_audit(
    cte_ref="mart_usage_event_plan_monthly",
    created_by="@dihle",
    updated_by="@utkarsh060",
    created_date="2022-02-22",
    updated_date="2024-01-31"
) }}
