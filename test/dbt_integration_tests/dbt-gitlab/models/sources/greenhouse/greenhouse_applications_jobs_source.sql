WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'applications_jobs') }}

), renamed as (

	SELECT
			--keys
    		application_id::NUMBER		AS application_id,
    		job_id::NUMBER				AS job_id

	FROM source

)

SELECT *
FROM renamed
