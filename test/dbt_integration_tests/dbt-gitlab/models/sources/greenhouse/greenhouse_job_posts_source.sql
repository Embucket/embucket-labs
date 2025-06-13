WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'job_posts') }}

), renamed as (

	SELECT

			--keys
   			id::NUMBER					      AS job_post_id,
   			job_id::NUMBER				    AS job_id,

   			--info
   			title::varchar				    AS job_post_title,
   			live::boolean				      AS is_job_live,
   			job_board_name::varchar		AS job_board_name,
   			language::varchar			    AS job_post_language,
   			location::varchar			    AS job_post_location,
   			created_at::timestamp 		AS job_post_created_at,
   			updated_at::timestamp 		AS job_post_updated_at

	FROM source

)

SELECT *
FROM renamed
