WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'offers') }}

), renamed as (

	SELECT
			id 											AS offer_id,

	    --keys
	    application_id,

	    --info
	    status 									AS offer_status,
	    created_by,
	    start_date::date 				AS start_date,

	    created_at::timestamp 	AS created_at,
	    sent_at::timestamp 			AS sent_at,
	    resolved_at::timestamp 	AS resolved_at,
	    updated_at::timestamp 	AS updated_at

	FROM source
	WHERE offer_status != 'deprecated'

)

SELECT *
FROM renamed
