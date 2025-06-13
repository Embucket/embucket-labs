WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_pto_source') }}
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
),

report AS (
  SELECT
    {{ dbt_utils.star(from=ref('gitlab_pto_source'), except=["HR_EMPLOYEE_ID"]) }},
    CASE source.pto_status
      WHEN 'AP' THEN 'Approved'
      WHEN 'DN' THEN 'Denied'
      WHEN 'RQ' THEN 'Requested'
      WHEN 'CN' THEN 'Cancelled'
    END                AS pto_status_name,
    COALESCE(map.wk_employee_id, source.hr_employee_id) AS hr_employee_id
  FROM source
  LEFT JOIN map
    ON source.hr_employee_id = map.bhr_employee_id
),

pto AS (
  SELECT *
  FROM report
),

prep_date AS (
  SELECT *
  FROM {{ ref('prep_date') }}
),

final AS (
  SELECT
    pto.*,
    prep_date.day_of_week AS pto_day_of_week,
    'Y'                   AS is_pto_date,
    IFF(
      pto.pto_type_name = 'Out Sick'
      AND DATEDIFF('day', pto.start_date, pto.end_date) > 4, 'Out Sick-Extended', pto.pto_type_name
    )                     AS absence_status
  FROM pto
  LEFT JOIN prep_date
    ON pto.pto_date = prep_date.date_actual
  WHERE pto.pto_status = 'AP'
    AND NOT prep_date.day_name in ('Sat','Sun')
)

SELECT *
FROM final
