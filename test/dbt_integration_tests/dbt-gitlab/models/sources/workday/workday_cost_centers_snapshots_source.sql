WITH source AS (

  SELECT *
  FROM {{ ref('cost_centers_snapshots') }}

),

final AS (

  SELECT
    report_effective_date::DATE                  AS report_effective_date,
    dept_workday_id::VARCHAR                     AS dept_workday_id,
    department_name::VARCHAR                     AS department_name,
    division::VARCHAR                            AS division,
    division_workday_id::VARCHAR                 AS division_workday_id,
    cost_center_workday_id::VARCHAR              AS cost_center_workday_id,
    cost_center::VARCHAR                         AS cost_center,
    dept_inactive::BOOLEAN                       AS dept_inactive,
    IFF(dept_inactive::BOOLEAN = 0, TRUE, FALSE) AS is_dept_active,
    dbt_valid_from::TIMESTAMP                    AS valid_from,
    dbt_valid_to::TIMESTAMP                      AS valid_to
  FROM source

)

SELECT *
FROM final
