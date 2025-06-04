WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'rejection_reasons') }}

), renamed as (

	SELECT
    		--keys
    		id::NUMBER								AS rejection_reason_id,
    		organization_id::NUMBER		AS organization_id,

    		--info
    		name::varchar							AS rejection_reason_name,
    		type::varchar   					AS rejection_reason_type,
    		created_at::timestamp 		AS rejection_reason_created_at,
    		updated_at::timestamp 		AS rejection_reason_updated_at


	FROM source

)

SELECT *
FROM renamed
