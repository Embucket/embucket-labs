WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_offices') }}

), renamed as (

	SELECT

            --keys
            id::NUMBER              AS job_office_id,
            job_id::NUMBER          AS job_id,
            office_id::NUMBER       AS office_id,

            --info
            created_at::timestamp   AS job_office_created_at,
            updated_at::timestamp   AS job_office_updated_at

	FROM source

)

SELECT *
FROM renamed
