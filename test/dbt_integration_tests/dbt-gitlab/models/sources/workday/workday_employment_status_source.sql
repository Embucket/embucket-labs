WITH source AS (
  SELECT *
  FROM {{ source('workday','employment_status') }}
  WHERE NOT _fivetran_deleted
  
),

renamed AS (

  SELECT
    source.employee_id::NUMBER                      AS employee_id,
    source._fivetran_synced::TIMESTAMP              AS uploaded_at,
    events.value['EFFECTIVE_DATE']::DATE            AS effective_date,
    events.value['EMPLOYMENT_STATUS']::VARCHAR      AS employment_status,
    events.value['TERMINATION_REASON']::VARCHAR     AS termination_reason,
    events.value['REGRETTABLE_STATUS']::VARCHAR     AS exit_impact,
    events.value['TERMINATION_TYPE']::VARCHAR       AS termination_type,
    events.value['DATE_TIME_INITIATED']::TIMESTAMP  AS initiated_at
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.employment_status_group) AS events

)

SELECT *
FROM renamed
