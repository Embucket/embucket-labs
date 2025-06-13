WITH sheetload_osat AS (

    SELECT *
    FROM {{ ref ('sheetload_osat') }}

), intermediate AS (

    SELECT 
      DATE_TRUNC(month, completed_date) AS completed_month,
      SUM(satisfaction_score)           AS osat_score,
      SUM(buddy_experience_score)       AS buddy_experience_score,
      COUNT(*)                          AS total_responses,
      COUNT(IFF(buddy_experience_score IS NOT NULL,1,0)) AS total_buddy_score_Responses
    FROM sheetload_osat
    GROUP BY 1
   
), rolling_3_month AS (

  SELECT *,
    SUM(osat_score) OVER (ORDER BY completed_month
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum_of_rolling_3_month_score,
    SUM(total_responses)  OVER (ORDER BY completed_month
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_month_respondents,
    SUM(buddy_experience_score) OVER (ORDER BY completed_month
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum_of_rolling_3_month_buddy_score,
    SUM(total_buddy_score_Responses)  OVER (ORDER BY completed_month 
                                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_month_buddy_respondents
  FROM intermediate
  
), final AS (

    SELECT *,
      sum_of_rolling_3_month_score / rolling_3_month_respondents                        AS rolling_3_month_osat,
      sum_of_rolling_3_month_buddy_score/ rolling_3_month_buddy_respondents             AS rolling_3_month_buddy_score
    FROM rolling_3_month

)

SELECT *
FROM final