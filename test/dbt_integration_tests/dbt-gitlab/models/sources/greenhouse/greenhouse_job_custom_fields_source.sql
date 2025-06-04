WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'job_custom_fields') }}

), renamed as (

	SELECT

			--key
   			job_id::NUMBER					      AS job_id,
		   	user_id::NUMBER					      AS user_id,

   			--info
   			custom_field::varchar			    AS job_custom_field,
   			float_value::float	          AS job_custom_field_float_value,
   			date_value::date 				      AS job_custom_field_date_value,
   			display_value::varchar			  AS job_custom_field_display_value,
   			unit::varchar					        AS job_custom_field_unit,
   			min_value::NUMBER				    AS job_custom_field_min_value,
   			max_value::NUMBER				    AS job_custom_field_max_value,
   			created_at::timestamp 			  AS job_custom_field_created_at,
   			updated_at::timestamp 			  AS job_custom_field_updated_at

	FROM source

)

SELECT *
FROM renamed
