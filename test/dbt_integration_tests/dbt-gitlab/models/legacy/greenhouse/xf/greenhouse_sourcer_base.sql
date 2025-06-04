WITH sourcer_metrics AS (

    SELECT 
      month_date,                                  
      sourcer_name,
      prospected,
      prospect_to_review,
      prospect_to_screen,
      app_reviewed,
      review_to_screen,
      screen,
      screen_to_interview,
      screen_to_hire,
      candidate_dropout
    FROM {{ ref ('greenhouse_sourcer_metrics') }} sourcer_metrics 
    WHERE part_of_recruiting_team = 1

), time_period AS (

    SELECT DISTINCT
      date_actual                                   AS reporting_month,
      DATEADD(month,-3,date_actual)                 AS start_period,    
      DATEADD(month,-1,date_actual)                 AS end_period          
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1
      AND date_actual BETWEEN DATE_TRUNC(month,DATEADD(month,-15,CURRENT_DATE())) AND DATE_TRUNC(month,CURRENT_DATE())
  
), three_month_rolling AS (

    SELECT 
      time_period.reporting_month,
      time_period.start_period,
      time_period.end_period,
      sourcer_metrics.*
    FROM time_period
    LEFT JOIN sourcer_metrics       
        ON sourcer_metrics.month_date BETWEEN time_period.start_period AND time_period.end_period
   
)

SELECT *
FROM three_month_rolling
