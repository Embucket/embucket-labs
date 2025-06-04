{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('mart_event_valid', 'mart_event_valid')
    ])
}},

/*
Look for a namespace's last event within a calendar month.
The plan on the final event will be used to attribute the namespace's usage during the month.
This is the same logic used in rpt_event_xmau_metric_monthly and rpt_event_plan_monthly, but
expanded to look at the entire calendar month instead of the last 28 days.
A flag will be added below to allow filtering based on the reporting period.
*/

plan_id_by_month AS (

  SELECT
    dim_ultimate_parent_namespace_id,
    event_calendar_month,
    plan_id_at_event_date,
    plan_name_at_event_date,
    plan_was_paid_at_event_date,
    namespace_is_internal,
    ultimate_parent_namespace_type,
    namespace_creator_is_blocked,
    event_created_at,
    event_date,
    event_pk
  FROM mart_event_valid
  WHERE event_date < DATE_TRUNC('month', CURRENT_DATE) --exclude current month
    AND dim_ultimate_parent_namespace_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_ultimate_parent_namespace_id, event_calendar_month
      ORDER BY event_created_at DESC) = 1

),

/*
Count and create a list of all plans that a namespace had during the calendar month
*/

plan_arrays AS (

  SELECT
    dim_ultimate_parent_namespace_id             AS ultimate_parent_namespace_id,
    event_calendar_month                         AS event_month,
    COUNT(DISTINCT plan_id_at_event_date)        AS plan_id_count,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT plan_id_at_event_date) WITHIN GROUP (ORDER BY plan_id_at_event_date),
      ',')                                       AS plan_id_list,
    ARRAY_TO_STRING(
      ARRAY_AGG(DISTINCT plan_name_at_event_date) WITHIN GROUP (ORDER BY plan_name_at_event_date),
      ',')                                       AS plan_name_list
  FROM mart_event_valid
  WHERE event_date < DATE_TRUNC('month', CURRENT_DATE) --exclude current month
  {{ dbt_utils.group_by(n=2) }}

),

/*
In PI reporting, we use the last 28 days of the month as the reporting period.
Since this looked at the entire calendar month, include a flag if a namespace's last 
event was during that period. 
That flag can then be used to have this model tie out to "official" reporting models,
rpt_event_xmau_metric_monthly and rpt_event_plan_monthly.
*/

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['event_calendar_month', 'dim_ultimate_parent_namespace_id']) }}         AS namespace_monthly_pk,
    event_calendar_month                                                                                AS event_calendar_month,
    dim_ultimate_parent_namespace_id                                                                    AS dim_ultimate_parent_namespace_id,
    plan_id_at_event_date                                                                               AS plan_id_at_event_month,
    plan_name_at_event_date                                                                             AS plan_name_at_event_month,
    plan_was_paid_at_event_date                                                                         AS plan_was_paid_at_event_month,
    plan_id_count                                                                                       AS plan_id_count,
    plan_id_list                                                                                        AS plan_id_list,
    plan_name_list                                                                                      AS plan_name_list,
    namespace_is_internal                                                                               AS namespace_is_internal,
    ultimate_parent_namespace_type                                                                      AS ultimate_parent_namespace_type,
    namespace_creator_is_blocked                                                                        AS namespace_creator_is_blocked,
    DATEADD('day', -27, LAST_DAY(event_calendar_month))                                                 AS first_day_of_reporting_period,
    LAST_DAY(event_calendar_month)                                                                      AS last_day_of_reporting_period,
    IFF(event_date BETWEEN first_day_of_reporting_period AND last_day_of_reporting_period, TRUE, FALSE) AS has_event_during_reporting_period
  FROM plan_id_by_month
  INNER JOIN plan_arrays
    ON plan_id_by_month.dim_ultimate_parent_namespace_id = plan_arrays.ultimate_parent_namespace_id
      AND plan_id_by_month.event_calendar_month = plan_arrays.event_month

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@cbraza",
    updated_by="@cbraza",
    created_date="2022-10-24",
    updated_date="2022-10-28"
) }}
