WITH source AS (

  SELECT *
  FROM {{ source('sheetload','cost_centers_historical') }}

),

renamed AS (

  SELECT
    dept_workday_id::VARCHAR                         AS dept_workday_id,
    dept_reference_id::VARCHAR                       AS dept_reference_id,
    division_workday_id::VARCHAR                     AS division_workday_id,
    cost_center_workday_id::VARCHAR                  AS cost_center_workday_id,
    department_name::VARCHAR                         AS department,
    division::VARCHAR                                AS division,
    division_refid::VARCHAR                          AS division_refid,
    cost_center::VARCHAR                             AS cost_center,
    cost_center_refid::VARCHAR                       AS cost_center_refid,
    IFF(dept_inactive::VARCHAR = 'Yes', FALSE, TRUE) AS is_department_active,
    report_effective_date::DATE                      AS report_effective_date

  FROM source

)

SELECT *
FROM renamed
