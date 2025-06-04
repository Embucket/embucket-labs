WITH source as (

	SELECT *
  	  FROM {{ source('greenhouse', 'jobs_interviews') }}

), renamed as (

	SELECT

            --keys
            id::NUMBER                  AS job_interview_id,
            job_id::NUMBER              AS job_id,
            stage_id::NUMBER            AS interview_stage_id,
            interview_id::NUMBER        AS interview_id,

            --info
            "order"::NUMBER                AS interview_order,
            estimated_duration::NUMBER     AS estimated_duration,
            created_at::timestamp       AS interview_created_at,
            updated_at::timestamp       AS interview_updated_at

	FROM source

)

SELECT *
FROM renamed
