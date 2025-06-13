WITH recruiting_data AS (
  
    SELECT *
    FROM {{ ref ('greenhouse_stage_analysis') }}

), isat AS (

    SELECT
      submitted_at,
      AVG(isat_score) AS isat
    FROM {{ ref ('rpt_interviewee_satisfaction_isat') }}
    GROUP BY 1

), metrics AS (

    SELECT 
      month_stage_entered_on                                                                           AS month_date,
      SUM(IFF(application_stage = 'Application Submitted',1,0))                                        AS prospected,
      IFF(prospected = 0, NULL, 
        (SUM(IFF(application_stage = 'Application Submitted',hit_application_review,0))/ prospected))  AS prospect_to_review,
      IFF(prospected = 0, NULL, 
          (SUM(IFF(application_stage = 'Application Submitted', hit_screening,0))/prospected))         AS prospect_to_screen,
      IFF(prospected = 0, NULL, 
          SUM(IFF(application_stage = 'Application Submitted',hit_hired,0))/prospected)                AS prospect_to_hire,
      IFF(prospected = 0, NULL,  
        SUM(IFF(application_stage = 'Application Submitted', candidate_dropout,0))/prospected)         AS prospect_to_dropout,
    
      SUM(IFF(application_stage = 'Application Review',1,0))                                           AS app_reviewed,
      IFF(app_reviewed = 0, NULL, 
            (SUM(IFF(application_stage = 'Application Review', hit_screening,0))/app_reviewed))        AS review_to_screen,
      IFF(app_reviewed = 0, NULL, 
            SUM(IFF(application_stage = 'Application Review', hit_hired,0))/app_reviewed)              AS review_to_hire,
    
      SUM(IFF(application_stage = 'Screen',1,0))                                                       AS screen,
      IFF(screen = 0, NULL, 
        SUM(IFF(application_stage = 'Screen', hit_team_interview,0))/screen)                           AS screen_to_interview,
      IFF(screen = 0, NULL, SUM(IFF(application_stage = 'Screen', hit_hired,0))/screen)                AS screen_to_hire,
    
      SUM(IFF(application_stage = 'Team Interview - Face to Face',1,0))                                 AS team_interview,
      IFF(team_interview = 0, NULL, 
          SUM(IFF(application_stage = 'Team Interview - Face to Face', hit_hired,0))/team_interview)    AS interview_to_hire,
      IFF(team_interview = 0, NULL, 
            SUM(IFF(application_stage = 'Team Interview - Face to Face', hit_rejected,0))/team_interview) AS interview_to_reject,

      SUM(IFF(application_stage = 'Executive Interview',1,0))                                           AS executive_interview,
      IFF(executive_interview = 0, NULL, 
                SUM(IFF(application_stage = 'Executive Interview', hit_hired,0))/executive_interview)   AS exec_interview_to_hire,
        
      SUM(IFF(application_stage = 'Reference Check',1,0))                                               AS reference_check,

      SUM(IFF(application_stage = 'Rejected', candidate_dropout,0))                                     AS candidate_dropout,

      SUM(IFF(application_stage = 'Offer',1,0))                                                         AS offer,
      IFF(offer = 0, NULL, 
        SUM(IFF(application_stage  ='Offer' AND application_Status ='hired',hit_hired,0))/offer)        AS offer_acceptance_rate,

      SUM(IFF(application_stage = 'Hired',1,0))                                                         AS hired, 
      SUM(IFF(application_stage = 'Hired' AND source_name != 'Internal Applicant',1,0))                 AS hires_excluding_transfers, 
    
    ---note hired includes interal applicants whereas hires_excluding_transfers

      MEDIAN(IFF(application_stage = 'Hired', time_to_offer, NULL))                                   AS time_to_offer_median,
      SUM(IFF(application_stage = 'Hired' AND is_sourced = 1,1,0))                                    AS sourced_candidate,
        
      IFF(hires_excluding_transfers =0, 0, sourced_candidate/hires_excluding_transfers)               AS percent_sourced_hires,
      SUM(IFF(application_stage = 'Hired' AND is_outbound = 1,1,0))                                   AS outbound_candidate,
      IFF(hires_excluding_transfers =0, 0, outbound_candidate/hires_excluding_transfers)              AS percent_outbound_hires

    FROM  recruiting_data
    WHERE unique_key NOT IN ('6d31c2d36d2eaec7f5b36605ac3ccf77')
    GROUP BY 1

), final AS (  

    SELECT 
      metrics.*,
      isat.isat
    FROM metrics
    LEFT JOIN isat
      ON isat.submitted_at = metrics.month_date
    WHERE month_date BETWEEN DATE_TRUNC(month, DATEADD(month,-13,CURRENT_DATE()))
                                     AND DATE_TRUNC(month, CURRENT_DATE())

)

SELECT *
FROM final