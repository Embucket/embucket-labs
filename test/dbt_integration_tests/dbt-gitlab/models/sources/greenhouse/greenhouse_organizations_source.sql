WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'organizations') }}

), renamed as (

	SELECT
 			--key
 			id::NUMBER			   AS organization_id,

 			--info
    		name::varchar		 AS organization_name


	FROM source

)

SELECT *
FROM renamed
