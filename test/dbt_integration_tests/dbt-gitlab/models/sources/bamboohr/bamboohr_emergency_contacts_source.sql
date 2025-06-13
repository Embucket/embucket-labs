WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'emergency_contacts') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT 
      d.value AS data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true) d
  
), renamed AS (

    SELECT
      data_by_row['employeeId']::NUMBER 		AS employee_id,
      data_by_row['id']::NUMBER 				AS emergency_contact_id,
	    data_by_row['name']::VARCHAR 	            AS full_name,
      data_by_row['homePhone']::VARCHAR 		AS home_phone,
      data_by_row['mobilePhone']::VARCHAR 		AS mobile_phone,
      data_by_row['workPhone']::VARCHAR			AS work_phone,
      uploaded_at
    FROM intermediate

)

SELECT *
FROM renamed
