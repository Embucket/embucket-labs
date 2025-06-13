WITH pto_source AS (

  SELECT *
  FROM {{ ref('prep_pto') }}

),

date_details AS (

  SELECT *
  FROM {{ ref('dim_date') }}

),

start_to_end AS (

  SELECT *
  FROM {{ ref('prep_team_member_history') }}

),

pto AS (

  SELECT
    *,
    DATEDIFF(DAY, start_date, end_date) + 1                AS pto_days_requested,
    NOT COALESCE(total_hours < employee_day_length, FALSE) AS is_full_day,
    ROW_NUMBER() OVER (
      PARTITION BY
        hr_employee_id,
        pto_date
      ORDER BY
        end_date DESC,
        pto_uuid DESC
    )                                                      AS pto_rank
  FROM pto_source
  WHERE pto_date <= CURRENT_DATE
    AND pto_days_requested <= 25
    AND COALESCE(pto_group_type, '') != 'EXL'
    AND NOT COALESCE(pto_type_name, '') IN ('CEO Shadow Program', 'Conference', 'Customer Visit')
  QUALIFY pto_rank = 1

),

dates AS (

  SELECT *
  FROM date_details

),

final AS (

  SELECT
    start_to_end.employee_id,
    start_to_end.hire_date,
    start_to_end.term_date,
    start_to_end.last_date,
    dates.date_actual,
    GREATEST(start_to_end.hire_date, DATEADD('month', -11, DATE_TRUNC('month', dates.date_actual))) AS r12_start_date,
    dates.fiscal_month_name_fy,
    dates.fiscal_quarter_name_fy,
    dates.fiscal_year,
    pto.pto_group_type,
    IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    )                                                                                               AS pto_type_name,
    pto.is_pto,
    COALESCE(pto.is_pto_date, 'N')                                                                  AS is_pto_date,
    IFF(pto.is_full_day = 'TRUE', 'Y', 'N')                                                         AS is_pto_full_day,
    IFF(pto.is_holiday = 'TRUE', 'Y', 'N')                                                          AS is_pto_holiday,
    IFF(pto.is_pto_date = 'Y', 1, 0)                                                                AS pto_count,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.employee_id,
        start_to_end.hire_date ORDER BY start_to_end.employee_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.employee_id,
        start_to_end.hire_date,
        dates.fiscal_month_name_fy ORDER BY start_to_end.employee_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum_mtd,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.employee_id,
        start_to_end.hire_date,
        dates.fiscal_quarter_name_fy ORDER BY start_to_end.employee_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum_qtd,
    SUM(pto_count) OVER (
      PARTITION BY
        start_to_end.employee_id,
        start_to_end.hire_date,
        dates.fiscal_year ORDER BY start_to_end.employee_id ASC,
      dates.date_actual ASC
    )                                                                                               AS running_pto_sum_ytd,
    COUNT(pto2.is_pto_date)                                                                         AS running_pto_sum_r12
  FROM start_to_end
  LEFT JOIN dates
    ON dates.date_actual BETWEEN start_to_end.hire_date
      AND start_to_end.last_date
  LEFT JOIN pto
    ON dates.date_actual = pto.pto_date
      AND start_to_end.employee_id = pto.hr_employee_id
  LEFT JOIN
    pto
      AS pto2
    ON start_to_end.employee_id = pto2.hr_employee_id
      AND dates.date_actual >= pto2.pto_date
      AND r12_start_date <= pto2.pto_date
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
  ORDER BY 1 ASC, 5 DESC
)

SELECT
  employee_id,
  hire_date,
  term_date,
  last_date,
  date_actual,
  is_pto_date,
  is_pto_full_day,
  is_pto_holiday,
  pto_group_type,
  pto_type_name,
  running_pto_sum,
  running_pto_sum_mtd,
  running_pto_sum_qtd,
  running_pto_sum_ytd,
  running_pto_sum_r12
FROM final
