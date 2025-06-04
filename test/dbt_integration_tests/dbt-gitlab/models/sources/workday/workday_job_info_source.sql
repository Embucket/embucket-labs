WITH source AS (
  SELECT *
  FROM {{ source('workday','job_info') }}
  WHERE NOT _fivetran_deleted
  
),

renamed AS (

  SELECT
    source.employee_id::NUMBER AS employee_id,
    source._fivetran_synced::TIMESTAMP AS uploaded_at,
    events.value['Business_Process_Event']::VARCHAR AS business_process_event,
    events.value['DEPARTMENT']::VARCHAR AS department,
    events.value['DIVISION']::VARCHAR AS division,
    events.value['EFFECTIVE_DATE']::DATE AS effective_date,
    events.value['ENTITY']::VARCHAR AS entity,
    events.value['JOB_TITLE']::VARCHAR AS job_title,
    events.value['REPORTS_TO']::VARCHAR AS reports_to,
    events.value['DATE_TIME_INITIATED']::TIMESTAMP AS initiated_at
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.job_info_group) AS events
)

SELECT *
FROM renamed
