WITH source as (

	SELECT *
  FROM {{ source('greenhouse', 'offer_custom_fields') }}

), renamed as (

	SELECT
   			--keys
   			offer_id::NUMBER				  AS offer_id,
   			user_id::NUMBER					  AS user_id,

   			--info
   			custom_field::varchar			AS offer_custom_field,
   			float_value::float	      AS offer_custom_field_float_value,
   			date_value::date 				  AS offer_custom_field_date,
   			display_value::varchar	  AS offer_custom_field_display_value,
   			unit::varchar					    AS offer_custom_field_unit,
   			min_value::NUMBER				AS offer_custom_field_min_value,
   			max_value::NUMBER				AS offer_custom_field_max_value,
   			created_at::timestamp			AS offer_custom_field_created_at,
   			updated_at::timestamp			AS offer_custom_field_updated_at

	FROM source

)

SELECT *
FROM renamed
