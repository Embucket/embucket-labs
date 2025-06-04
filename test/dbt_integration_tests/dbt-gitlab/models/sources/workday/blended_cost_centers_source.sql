WITH cost_centers_snapshots_source AS (
  SELECT *
  FROM {{ ref('workday_cost_centers_snapshots_source') }}
),

cost_centers_historical_source AS (
  SELECT *
  FROM {{ ref('sheetload_cost_centers_historical_source') }}
),

max_hist_date_cte AS (
  SELECT MAX(cost_centers_historical_source.report_effective_date) AS max_hist_date
  FROM cost_centers_historical_source
),

cost_centers_stage AS (
  SELECT
    dept_workday_id,
    cost_center_workday_id,
    department_name AS department,
    division,
    division_workday_id,
    cost_center,
    is_dept_active,
    report_effective_date
  FROM cost_centers_snapshots_source
  LEFT JOIN max_hist_date_cte
    ON cost_centers_snapshots_source.report_effective_date > max_hist_date_cte.max_hist_date

  UNION

  SELECT
    dept_workday_id,
    cost_center_workday_id,
    department,
    division,
    division_workday_id,
    cost_center,
    is_department_active AS is_dept_active,
    report_effective_date
  FROM cost_centers_historical_source
),

final AS (
  SELECT
    dept_workday_id,
    ROW_NUMBER() OVER (
      PARTITION BY dept_workday_id ORDER BY report_effective_date ASC
    )                                                             AS record_rank_asc,
    ROW_NUMBER() OVER (
      PARTITION BY dept_workday_id ORDER BY report_effective_date DESC
    )                                                             AS record_rank_desc,
    IFF(record_rank_asc = 1, '1900-01-01', report_effective_date) AS valid_from,
    COALESCE(LAG(report_effective_date) OVER (
      PARTITION BY dept_workday_id ORDER BY report_effective_date DESC
    ), '2099-01-01')                                              AS valid_to,
    cost_center_workday_id,
    division_workday_id,
    department,
    division,
    cost_center,
    is_dept_active AS is_department_active
  FROM cost_centers_stage
)

SELECT
  dept_workday_id,
  department,
  division_workday_id,
  division,
  cost_center_workday_id,
  cost_center,
  is_department_active,
  valid_from,
  valid_to
FROM final
