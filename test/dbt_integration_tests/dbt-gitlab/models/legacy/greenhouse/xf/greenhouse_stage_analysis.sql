{{ config({
    "materialized":"table",
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

 WITH greenhouse_stage_intermediate AS (

    SELECT *
    FROM {{ ref ('greenhouse_stage_intermediate') }}
 
 ), final AS (

    SELECT
      unique_key,
      application_stage, 
      is_milestone_stage,
      DATE_TRUNC(week, stage_entered_on) AS week_stage_entered_on,
      DATE_TRUNC(week, stage_exited_on) AS week_stage_exited_on,
      month_stage_entered_on,
      month_stage_exited_on,
      days_in_stage,
      days_between_stages,
      days_in_pipeline,
      row_number_stages_desc,
      next_stage,
      is_current_stage,
      application_month,
      job_id,
      requisition_id,
      is_prospect,
      current_stage_name,
      application_status,
      job_name,
      department_name,
      division_modified,
      source_name,
      source_type,
      is_outbound,
      is_sourced,
      sourcer_name,
      rejection_reason_name,
      rejection_reason_type,
      current_job_req_status,
      is_hired_in_bamboo,
      time_to_offer,
      hit_application_review,
      hit_assessment,
      hit_screening,
      hit_team_interview,
      hit_reference_check,
      hit_offer,
      hit_hired,
      hit_rejected,
      candidate_dropout,
      in_current_pipeline,
      turn_time_app_review_to_screen,
      turn_time_screen_to_interview,
      turn_time_interview_to_offer
    FROM greenhouse_stage_intermediate

 )

 SELECT *
 FROM final      
