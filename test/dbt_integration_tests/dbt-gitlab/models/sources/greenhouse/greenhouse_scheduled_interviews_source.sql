WITH source AS (

  SELECT *
  FROM {{ source('greenhouse', 'scheduled_interviews') }}

),

renamed AS (

  SELECT

    --keys
    id::NUMBER                        AS scheduled_interview_id,
    application_id::NUMBER            AS application_id,
    interview_id::NUMBER              AS interview_id,
    scheduled_by_id::NUMBER           AS interview_scheduled_by_id,

    --info
    status::VARCHAR                   AS scheduled_interview_status,
    scheduled_at::TIMESTAMP           AS interview_scheduled_at,
    starts_at::TIMESTAMP              AS interview_starts_at,
    ends_at::TIMESTAMP                AS interview_ends_at,
    all_day_start_date::VARCHAR::DATE AS all_day_start_date,
    all_day_end_date::VARCHAR::DATE   AS all_day_end_date,
    location::VARCHAR                 AS location,
    stage_name::VARCHAR               AS scheduled_interview_stage_name,
    interview_name::VARCHAR           AS scheduled_interview_name


  FROM source

)

SELECT *
FROM renamed
