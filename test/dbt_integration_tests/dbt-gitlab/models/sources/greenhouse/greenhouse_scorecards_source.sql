WITH source AS (

  SELECT *
  FROM {{ source('greenhouse', 'scorecards') }}

),

renamed AS (

  SELECT

    --keys
    id::NUMBER                              AS scorecard_id,
    application_id::NUMBER                  AS application_id,
    stage_id::NUMBER                        AS stage_id,
    interview_id::NUMBER                    AS interview_id,
    interviewer_id::NUMBER                  AS interviewer_id,
    submitter_id::NUMBER                    AS submitter_id,

    --info
    key_takeaways::VARCHAR                  AS key_takeaways,
    overall_recommendation::VARCHAR         AS scorecard_overall_recommendation,
    submitted_at::TIMESTAMP                 AS scorecard_submitted_at,
    scheduled_interview_ended_at::TIMESTAMP AS scorecard_scheduled_interview_ended_at,
    total_focus_attributes::NUMBER          AS scorecard_total_focus_attributes,
    completed_focus_attributes::NUMBER      AS scorecard_completed_focus_attributes,
    stage_name::VARCHAR                     AS scorecard_stage_name,
    created_at::TIMESTAMP                   AS scorecard_created_at,
    updated_at::TIMESTAMP                   AS scorecard_updated_at,
    interview_name::VARCHAR                 AS interview_name,
    interviewer::VARCHAR                    AS interviewer,
    submitter::VARCHAR                      AS scorecard_submitter

  FROM source

)

SELECT *
FROM renamed
