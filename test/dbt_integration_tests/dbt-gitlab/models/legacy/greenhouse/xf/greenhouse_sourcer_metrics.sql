WITH date_details AS (

    SELECT 
      date_actual   AS month_date,                             
      1             AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual BETWEEN DATE_TRUNC(month, DATEADD(month,-5,CURRENT_DATE())) AND CURRENT_DATE()
      AND day_of_month = 1 

), recruiting_team AS (

    SELECT DISTINCT 
      DATE_TRUNC(month, date_actual)            AS month_date,
      full_name,
      department
    FROM {{ ref ('employee_directory_analysis') }}
    WHERE department LIKE '%Recruiting%'
      AND date_actual BETWEEN DATE_TRUNC(month, DATEADD(month, -12,CURRENT_DATE())) AND DATE_TRUNC(month, CURRENT_DATE())

), recruiting_data AS (
    
    SELECT *,
      1 AS join_field
    FROM {{ ref ('greenhouse_stage_analysis') }}
    WHERE sourcer_name IS NOT NULL
      AND source_type = 'Prospecting'

), base AS (

    SELECT 
      date_details.month_date,
      recruiting_data.sourcer_name,
      IFF(recruiting_team.full_name IS NOT NULL, 1,0)       AS part_of_recruiting_team
    FROM date_details
    LEFT JOIN recruiting_data
      ON date_details.join_field = recruiting_data.join_field
    LEFT JOIN recruiting_team
      ON date_details.month_date = recruiting_team.month_date
      AND recruiting_data.sourcer_name = recruiting_team.full_name
    GROUP BY 1,2,3

), metrics AS (

    SELECT 
      month_date,
      base.sourcer_name,
      base.part_of_recruiting_team,
      SUM(IFF(application_stage = 'Application Submitted',1,0))                                                             AS prospected,
      IFF(prospected = 0, NULL, 
            (SUM(IFF(application_stage = 'Application Submitted',hit_application_review,0))/ prospected))                   AS prospect_to_review,
      IFF(prospected = 0, NULL, (SUM(IFF(application_stage = 'Application Submitted', hit_screening,0))/prospected))        AS prospect_to_screen,
      
      IFF(prospected = 0, NULL, SUM(IFF(application_stage = 'Application Submitted',hit_hired,0))/prospected)               AS prospect_to_hire,
      IFF(prospected = 0, NULL,  SUM(IFF(application_stage = 'Application Submitted', candidate_dropout,0))/prospected)     AS prospect_to_dropout,

      SUM(IFF(application_stage = 'Application Review',1,0))                                                                AS app_reviewed,
      IFF(app_reviewed = 0, NULL, (SUM(IFF(application_stage = 'Application Review', hit_screening,0))/app_reviewed))       AS review_to_screen,
      IFF(app_reviewed = 0, NULL, SUM(IFF(application_stage = 'Application Review', hit_hired,0))/app_reviewed)             AS review_to_hire,
  

      SUM(IFF(application_stage = 'Screen',1,0))                                                                            AS screen,
      IFF(screen = 0, NULL, SUM(IFF(application_stage = 'Screen', hit_team_interview,0))/screen)                            AS screen_to_interview,
      IFF(screen = 0, NULL, SUM(IFF(application_stage = 'Screen', hit_hired,0))/screen)                                     AS screen_to_hire,
  

      SUM(IFF(application_stage = 'Team Interview - Face to Face',1,0))                                                     AS team_interview,
      IFF(team_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Team Interview - Face to Face', hit_hired,0))/team_interview)                      AS interview_to_hire,
      IFF(team_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Team Interview - Face to Face', hit_rejected,0))/team_interview)                   AS interview_to_reject,

      SUM(IFF(application_stage = 'Executive Interview',1,0))                                                               AS executive_interview,
      IFF(executive_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Executive Interview', hit_hired,0))/executive_interview)                           AS exec_interview_to_hire,
    
      SUM(IFF(application_stage = 'Reference Check',1,0))                                                                   AS reference_check,

      SUM(IFF(application_stage = 'Rejected', candidate_dropout,0))                                                         AS candidate_dropout,

      SUM(IFF(application_stage = 'Offer',1,0))                                                                             AS offer,
      IFF(offer = 0, NULL, SUM(IFF(application_stage  ='Offer',hit_hired,0))/offer)                                         AS ofer_to_hire,

      SUM(IFF(application_stage = 'Hired',1,0))                                                                             AS hired, 

      MEDIAN(IFF(application_stage = 'Hired', time_to_offer, NULL))                                                         AS time_to_offer_median
    FROM base
    LEFT JOIN recruiting_data
      ON base.month_date = recruiting_data.month_stage_entered_on
      AND base.sourcer_name = recruiting_data.sourcer_name
    GROUP BY 1,2,3
  
)

SELECT *
FROM metrics