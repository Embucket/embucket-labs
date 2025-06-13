WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'opening_custom_fields') }}


), renamed as (

	SELECT

	  --key
   	  opening_id::varchar				  AS opening_id,

   	  --info
   	  key::varchar			                  AS opening_custom_field,
   	  display_value::varchar			  AS opening_custom_field_display_value,
   	  created_at::timestamp 			  AS opening_custom_field_created_at,
   	  updated_at::timestamp 			  AS opening_custom_field_updated_at
	FROM source

)

SELECT *
FROM renamed

