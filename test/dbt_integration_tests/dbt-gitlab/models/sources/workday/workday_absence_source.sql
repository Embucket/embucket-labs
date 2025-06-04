WITH source AS (

  SELECT *
  FROM {{ source('workday','time_off') }}
  WHERE NOT _fivetran_deleted

),

absence AS (

  SELECT
    employee_id,
    worker_workday_id,
    d.value['Absence_Workday_ID']::VARCHAR      AS absence_workday_id,
    d.value['Time_Off_Date']::DATE              AS time_off_date,
    d.value['Time_Off_Type']::VARCHAR           AS time_off_type,
    d.value['Reason']::VARCHAR                  AS time_off_reason,
    d.value['Hours_Taken']::NUMERIC             AS hours_taken,
    d.value['Scheduled_Work_Hours']::NUMERIC    AS scheduled_work_hours,
    d.value['FTE']::FLOAT                       AS fte_percent,
    d.value['Date_and_Time_Approved']::DATETIME AS datetime_approved,
    d.value['Country']::VARCHAR                 AS country,
    _fivetran_synced,
    _fivetran_deleted
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(time_off_details_group)) AS d

)

SELECT *
FROM absence
WHERE hours_taken > 0            --0 for cancelled requests
  AND scheduled_work_hours > 0   --0 for team members who terminated prior to time off date
