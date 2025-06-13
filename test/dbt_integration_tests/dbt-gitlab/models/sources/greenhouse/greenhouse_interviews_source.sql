WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'interviews') }}

), renamed as (

	SELECT

			--keys
    		id::NUMBER				              AS interview_id,
    		organization_id::NUMBER         AS organization_id,

    		--info
    		name::varchar			              AS interview_name,
    		created_at::varchar::timestamp 	AS interview_created_at,
    		updated_at::varchar::timestamp 	AS interview_updated_at

	FROM source

)

SELECT *
FROM renamed
