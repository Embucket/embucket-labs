
  
    

create or replace transient table EMBUCKET.greenhouse.greenhouse_applications_jobs_source
    

    
    as (WITH source as (

	SELECT *
  	FROM EMBUCKET.greenhouse.applications_jobs

), renamed as (

	SELECT
			--keys
    		application_id::NUMBER		AS application_id,
    		job_id::NUMBER				AS job_id

	FROM source

)

SELECT *
FROM renamed
    )
;


  