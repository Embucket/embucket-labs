WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'job_snapshots') }}

), renamed as (

	SELECT

			--key
    		job_id::NUMBER			AS job_id,

    		--info
    		date::date			    AS job_snapshot_date,
    		hired_count::NUMBER 		AS hired_count,
    		prospect_count::NUMBER AS prospect_count,
    		new_today::NUMBER 			AS new_today,
    		rejected_today::NUMBER AS rejected_today,
    		advanced_today::NUMBER AS advanced_today,
    		interviews_today::NUMBER AS interviews_today

	FROM source

)

SELECT *
FROM renamed
