WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'job_info') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT 
        d.value AS data_by_row,
        uploaded_at
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT 
      data_by_row['id']::NUMBER              AS job_id,
      data_by_row['employeeId']::NUMBER      AS employee_id,
      data_by_row['jobTitle']::VARCHAR       AS job_title,
      data_by_row['date']::DATE              AS effective_date,
      data_by_row['department']::VARCHAR     AS department,
      data_by_row['division']::VARCHAR       AS division,
      data_by_row['location']::VARCHAR       AS entity,
      data_by_row['reportsTo']::VARCHAR      AS reports_to,
      uploaded_at
    FROM intermediate

)

SELECT *
FROM renamed
