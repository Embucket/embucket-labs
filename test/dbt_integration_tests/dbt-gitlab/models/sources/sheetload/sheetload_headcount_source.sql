WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'headcount') }}

), renamed AS (


    SELECT 
      uniquekey::NUMBER                 AS primary_key,
      month::DATE                       AS month_of,
      NULLIF(function, '')              AS function,
      TRY_TO_NUMBER(employee_cnt)       AS employee_count
    FROM source

)

SELECT *
FROM renamed
