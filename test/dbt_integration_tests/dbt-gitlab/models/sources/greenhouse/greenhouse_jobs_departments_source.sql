WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_departments') }}

), renamed as (

	SELECT

			--keys
    		id::NUMBER				      AS job_department_id,
    		job_id::NUMBER			    AS job_id,
    		department_id::NUMBER	  AS department_id,

    		--info
    		created_at::timestamp 	AS job_department_created_at,
    		updated_at::timestamp 	AS job_department_updated_at


	FROM source

)

SELECT *
FROM renamed
