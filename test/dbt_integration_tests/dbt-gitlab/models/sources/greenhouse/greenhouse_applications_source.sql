WITH source AS (

	SELECT *
  	FROM {{ source('greenhouse', 'applications') }}

), stages_source AS (

    SELECT * 
    FROM {{ source('greenhouse', 'application_stages') }}

), stages AS (

    SELECT * 
    FROM stages_source
    WHERE entered_on IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY entered_on DESC) =1

), renamed as (

	SELECT  id 						        AS application_id,

			--keys
		    candidate_id,
		    stages.stage_id,
		    source_id,
		    referrer_id,
		    rejected_by_id,
		    job_post_id,
		    event_id,
		    rejection_reason_id,
		    converted_prospect_application_id,
        recruiter_id            AS application_recruiter_id,
        coordinator_id          AS application_coordinator_id,

		    --info
		    status 					        AS application_status,
		    prospect,
        recruiter               AS application_recruiter,
        coordinator             AS application_coordinator,
		    pipeline_percent,
		    migrated,
		    rejected_by,
		    stages.stage_name,
		    prospect_pool,
		    prospect_pool_stage,

		    applied_at::timestamp 	AS applied_at,
		    rejected_at::timestamp 	AS rejected_at,
		    created_at::timestamp 	AS created_at,
		    updated_at::timestamp 	AS last_updated_at
	FROM source
    LEFT JOIN stages 
      ON stages.application_id = source.id
)

SELECT *
FROM renamed
