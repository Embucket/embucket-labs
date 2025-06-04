WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'referrers') }}

), renamed as (

	SELECT  id 											AS referrer_id,
          name          					AS referrer_name,
    			--keys
          organization_id,
          user_id,

          created_at::timestamp 	AS created_at,
          updated_at::timestamp 	AS updated_at

	FROM source

)

SELECT *
FROM renamed
