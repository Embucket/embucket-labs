{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_date','dim_date'),
    ('mart_event_valid', 'mart_event_valid'),
    ('xmau_metrics', 'map_gitlab_dotcom_xmau_metrics')
    ])
}},

mart_raw AS (

  SELECT
    {{ dbt_utils.star(from=ref('mart_event_valid'), except=["STAGE_NAME"]) }},
    CASE
      WHEN stage_name = 'manage' THEN NULL
      ELSE stage_name
    END AS stage_name
  FROM mart_event_valid
  WHERE is_null_user = FALSE
    AND (is_umau = TRUE
         OR is_gmau = TRUE
         OR is_smau = TRUE
        )

),

mart_with_date_range AS (

  SELECT
    mart_raw.*,
    dim_date.last_day_of_month,
    dim_date.last_day_of_quarter,
    dim_date.last_day_of_fiscal_year
  FROM mart_raw
  LEFT JOIN dim_date
    ON mart_raw.event_date = dim_date.date_actual
  WHERE mart_raw.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

),

paid_flag_by_month AS (

  SELECT
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    plan_was_paid_at_event_date
  FROM mart_with_date_range
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_calendar_month
      ORDER BY event_created_at DESC) = 1

),

multiple_gmau_in_smau AS (
  --Find stages that have SMAU metrics that span multiple groups OR contain both GMAU and non-GMAU events
  SELECT
    stage_name,
    COUNT(DISTINCT group_name) AS group_count, --count of groups included in smau
    COUNT(DISTINCT gmau) AS gmau_flag_count --count of distinct GMAU flags in SMAU events
  FROM xmau_metrics
  WHERE smau = TRUE --stage's smau event
  GROUP BY 1
  HAVING group_count > 1 --more than 1 group in SMAU
    OR gmau_flag_count > 1 --SMAU includes both GMAU and non-GMAU events

),

mart_w_paid_deduped AS (

  SELECT
    mart_with_date_range.event_pk,
    mart_with_date_range.event_date,
    mart_with_date_range.last_day_of_month,
    mart_with_date_range.last_day_of_quarter,
    mart_with_date_range.last_day_of_fiscal_year,
    mart_with_date_range.dim_user_id,
    mart_with_date_range.event_name,
    mart_with_date_range.data_source,
    mart_with_date_range.dim_ultimate_parent_namespace_id,
    mart_with_date_range.is_umau,
    mart_with_date_range.is_gmau,
    CASE
      WHEN multiple_gmau_in_smau.stage_name IS NOT NULL --SMAU contains events across multiple groups OR includes GMAU and non-GMAU metrics
      THEN FALSE
      ELSE mart_with_date_range.is_smau
    END AS is_smau,
    mart_with_date_range.section_name,
    mart_with_date_range.stage_name,
    mart_with_date_range.group_name,
    mart_with_date_range.event_calendar_month,
    mart_with_date_range.event_calendar_quarter,
    mart_with_date_range.event_calendar_year,
    paid_flag_by_month.plan_was_paid_at_event_date
  FROM mart_with_date_range
  LEFT JOIN paid_flag_by_month
    ON mart_with_date_range.dim_ultimate_parent_namespace_id = paid_flag_by_month.dim_ultimate_parent_namespace_id
      AND mart_with_date_range.event_calendar_month = paid_flag_by_month.event_calendar_month
  LEFT JOIN multiple_gmau_in_smau
    ON mart_with_date_range.stage_name = multiple_gmau_in_smau.stage_name

),

mart_for_multiple_gmau_in_smau AS (

  SELECT
    mart_with_date_range.event_pk,
    mart_with_date_range.event_date,
    mart_with_date_range.last_day_of_month,
    mart_with_date_range.last_day_of_quarter,
    mart_with_date_range.last_day_of_fiscal_year,
    mart_with_date_range.dim_user_id,
    mart_with_date_range.event_name,
    mart_with_date_range.data_source,
    mart_with_date_range.dim_ultimate_parent_namespace_id,
    mart_with_date_range.is_umau,
    FALSE AS is_gmau,
    mart_with_date_range.is_smau,
    mart_with_date_range.section_name,
    mart_with_date_range.stage_name,
    NULL AS group_name,
    mart_with_date_range.event_calendar_month,
    mart_with_date_range.event_calendar_quarter,
    mart_with_date_range.event_calendar_year,
    paid_flag_by_month.plan_was_paid_at_event_date
  FROM mart_with_date_range
  LEFT JOIN paid_flag_by_month
    ON mart_with_date_range.dim_ultimate_parent_namespace_id = paid_flag_by_month.dim_ultimate_parent_namespace_id
      AND mart_with_date_range.event_calendar_month = paid_flag_by_month.event_calendar_month
  INNER JOIN multiple_gmau_in_smau --only want stages in this CTE
    ON mart_with_date_range.stage_name = multiple_gmau_in_smau.stage_name
  WHERE mart_with_date_range.is_smau = TRUE --only want SMAU events


),

final_mart AS (

  SELECT *
  FROM mart_w_paid_deduped
  WHERE is_smau = TRUE
    OR is_gmau = TRUE
    OR is_umau = TRUE

  UNION ALL

  SELECT *
  FROM mart_for_multiple_gmau_in_smau

),

total_results AS (

  SELECT
    event_calendar_month,
    is_umau,
    is_gmau,
    is_smau,
    section_name,
    stage_name,
    group_name,
    'total' AS user_group,
    ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name) AS event_name_array,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM final_mart
  {{ dbt_utils.group_by(n=8) }}
  ORDER BY event_calendar_month DESC

),

free_results AS (

  SELECT
    event_calendar_month,
    is_umau,
    is_gmau,
    is_smau,
    section_name,
    stage_name,
    group_name,
    'free' AS user_group,
    ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name) AS event_name_array,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM final_mart
  WHERE plan_was_paid_at_event_date = FALSE
  {{ dbt_utils.group_by(n=8) }}
  ORDER BY event_calendar_month DESC

),

paid_results AS (

  SELECT
    event_calendar_month,
    is_umau,
    is_gmau,
    is_smau,
    section_name,
    stage_name,
    group_name,
    'paid' AS user_group,
    ARRAY_AGG(DISTINCT event_name) WITHIN GROUP (ORDER BY event_name) AS event_name_array,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count
  FROM final_mart
  WHERE plan_was_paid_at_event_date = TRUE
  {{ dbt_utils.group_by(n=8) }}
  ORDER BY event_calendar_month DESC

),

results_wo_pk AS (

  SELECT *
  FROM total_results

  UNION ALL

  SELECT *
  FROM free_results

  UNION ALL

  SELECT *
  FROM paid_results

),

results AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['event_calendar_month', 'user_group', 'event_name_array']) }} AS xmau_metric_monthly_id,
    results_wo_pk.*
  FROM results_wo_pk
  WHERE event_calendar_month < DATE_TRUNC('month', CURRENT_DATE)

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper_acp",
    updated_by="@cbraza",
    created_date="2022-02-23",
    updated_date="2023-05-24"
) }}
