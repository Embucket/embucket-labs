WITH source AS (

  SELECT *
  FROM {{ source('workday','cost_centers') }}
  WHERE NOT _fivetran_deleted

),

renamed AS (

  SELECT
    dept_workday_id::VARCHAR           AS department_workday_id,
    dept_reference_id::VARCHAR         AS department_refid,
    division_workday_id::VARCHAR       AS division_workday_id,
    cost_center_workday_id::VARCHAR    AS cost_center_workday_id,
    department_name::VARCHAR           AS department_name,
    cost_center::VARCHAR               AS cost_center,
    division_refid::VARCHAR            AS division_refid,
    division::VARCHAR                  AS division,
    cost_center_refid::VARCHAR         AS cost_center_refid,
    dept_inactive::BOOLEAN             AS is_department_inactive
  FROM source

)

SELECT *
FROM renamed