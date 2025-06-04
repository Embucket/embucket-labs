WITH source as (

	SELECT *
  	FROM {{ source('greenhouse', 'jobs_stages') }}

), renamed as (

	SELECT
            --keys
            job_id::NUMBER                  AS job_id,
            stage_id::NUMBER                AS job_stage_id,

            --info
            "order"::NUMBER                    AS job_stage_order,
            name::varchar                   AS job_stage_name,
            stage_alert_setting::varchar    AS job_stage_alert_setting,
            created_at::timestamp           AS job_stage_created_at,
            updated_at::timestamp           AS job_stage_updated_at,
            milestones::varchar             AS job_stage_milestone

	FROM source

)

SELECT *
FROM renamed
