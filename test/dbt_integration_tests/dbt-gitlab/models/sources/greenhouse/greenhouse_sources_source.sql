WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'sources') }}

), renamed as (

	SELECT id 									AS source_id,

		--keys
	    organization_id,
	    name::VARCHAR(250) 						AS source_name,
	    type::VARCHAR(250) 						AS source_type,

	    created_at::timestamp 	AS created_at,
	    updated_at::timestamp 	AS updated_at

	FROM source
)

SELECT *
FROM renamed
