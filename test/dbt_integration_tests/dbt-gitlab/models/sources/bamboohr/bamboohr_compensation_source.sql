WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'compensation') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT 
      d.value as data_by_row, 
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT
      data_by_row['id']::NUMBER                AS compensation_update_id,
      data_by_row['employeeId']::NUMBER        AS employee_id,
      data_by_row['startDate']::DATE           AS effective_date,
      data_by_row['type']::VARCHAR             AS compensation_type,
      data_by_row['reason']::VARCHAR           AS compensation_change_reason,
      data_by_row['paidPer']::VARCHAR          AS pay_rate,   
      TRY_TO_NUMBER(data_by_row['rate']['value']::VARCHAR)      AS compensation_value,
      data_by_row['rate']['currency']::VARCHAR AS compensation_currency,
      uploaded_at 
    FROM intermediate
    -- Filter out specific team member that was brought in as a contractor without a compensation value.
    WHERE (employee_id != 43749 OR compensation_value IS NOT NULL)
      
)

SELECT
  compensation_update_id,
  employee_id,
  effective_date,
  compensation_type,
  compensation_change_reason,
  pay_rate,
  IFF(compensation_type = 'Hourly', compensation_value * 80, compensation_value) AS compensation_value,
  compensation_currency,
  uploaded_at
FROM renamed
