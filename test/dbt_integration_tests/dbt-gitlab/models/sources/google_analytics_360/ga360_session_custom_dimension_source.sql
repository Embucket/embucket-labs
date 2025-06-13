WITH source AS (

	SELECT *
	FROM {{ source('google_analytics_360', 'ga_session_custom_dimension') }}

), renamed AS(

	SELECT
	--Keys
	  index::FLOAT                      AS dimension_index,
	  visit_id::FLOAT                   AS visit_id,
	  visitor_id::VARCHAR               AS visitor_id,
	  
	  --Info
	  value::VARCHAR                    AS dimension_value,
	  visit_start_time::TIMESTAMP_TZ    AS visit_start_time

	FROM source

)

SELECT *
FROM renamed
