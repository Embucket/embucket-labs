WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'hiring_team') }}

), renamed as (

	SELECT

			--keys
   			job_id::NUMBER				  AS job_id,
    		user_id::NUMBER				  AS user_id,

    		--info
    		role::varchar				    AS hiring_team_role,
    		responsible::boolean		AS is_responsible,
    		created_at::timestamp		AS hiring_team_created_at,
    		updated_at::timestamp		AS hiring_team_updated_at

	FROM source

)

SELECT *
FROM renamed