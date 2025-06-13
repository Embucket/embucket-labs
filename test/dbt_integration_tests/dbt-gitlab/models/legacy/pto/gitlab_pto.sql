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
    CASE pto_status
      WHEN 'AP' THEN 'Approved'
      WHEN 'DN' THEN 'Denied'
      WHEN 'RQ' THEN 'Requested'
      WHEN 'CN' THEN 'Cancelled'
    END AS pto_status_name,
    COALESCE(map.wk_employee_id, source.hr_employee_id) AS hr_employee_id
  FROM source
  LEFT JOIN map
    ON source.hr_employee_id = map.bhr_employee_id
)

SELECT *
FROM report
