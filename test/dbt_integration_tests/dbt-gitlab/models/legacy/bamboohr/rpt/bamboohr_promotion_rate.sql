WITH promotions AS (

    SELECT *
    FROM {{ ref('bamboohr_promotions_xf') }}

), bamboohr_base AS (

    SELECT
      DATEADD(month, -11, date_actual)                           AS rolling_start_month,
      date_actual                                                AS rolling_end_month,
      field_name,
      field_value
    FROM {{ ref('bamboohr_base_mapping') }}
  
), headcount_end AS (

    SELECT 
      month_date, 
      CASE WHEN breakout_type = 'kpi_breakout'
            THEN 'company_breakout'
           WHEN breakout_type = 'department_breakout'
            THEN 'department_grouping_breakout'
           WHEN breakout_type ='division_breakout'
            THEN 'division_grouping_breakout' 
           ELSE NULL END                                                        AS breakout_type, 
      CASE WHEN breakout_type = 'kpi_breakout' 
             THEN 'company_breakout'
           WHEN breakout_type = 'division_breakout' 
             THEN {{ bamboohr_division_grouping(division='division') }}
           ELSE {{ bamboohr_department_grouping(department='department') }} END AS division_department, 
      SUM(headcount_end)                                                        AS headcount_end,
      SUM(headcount_end_excluding_sdr)                                          AS headcount_end_excluding_sdr,
      SUM(rolling_12_month_headcount)                                           AS rolling_12_month_headcount,
      SUM(rolling_12_month_headcount_excluding_sdr)                             AS rolling_12_month_headcount_excluding_sdr
    FROM {{ ref('bamboohr_rpt_headcount_aggregation') }}  
    WHERE breakout_type IN ('department_breakout', 'kpi_breakout', 'division_breakout')
        AND eeoc_field_name = 'no_eeoc'
    GROUP BY 1,2,3    
  

), joined AS (

    SELECT 
      bamboohr_base.*, 
      promotions.*, 
      headcount_end,
      rolling_12_month_headcount
    FROM bamboohr_base
    LEFT JOIN promotions
      ON promotions.promotion_month BETWEEN rolling_start_month AND rolling_end_month
      AND IFF(field_name = 'division_grouping_breakout', promotions.division_grouping, promotions.department_grouping) = bamboohr_base.field_value
     LEFT JOIN headcount_end
      ON bamboohr_base.rolling_end_month = headcount_end.month_date
      AND bamboohr_base.field_name = headcount_end.breakout_type
      AND bamboohr_base.field_value = headcount_end.division_department
    WHERE bamboohr_base.field_name !='company_breakout'

    UNION ALL

    SELECT 
      bamboohr_base.rolling_start_month,
      bamboohr_base.rolling_end_month,
      'division_grouping_breakout' AS field_name,
      'Marketing - Excluding SDR' AS field_value,
      promotions.*, 
      headcount_end_excluding_sdr,
      rolling_12_month_headcount_excluding_sdr 
    FROM bamboohr_base
    INNER JOIN promotions
      ON promotions.promotion_month BETWEEN rolling_start_month AND rolling_end_month
      AND IFF(field_name = 'division_grouping_breakout', promotions.division_grouping, promotions.department_grouping) = bamboohr_base.field_value
    LEFT JOIN headcount_end
      ON bamboohr_base.rolling_end_month = headcount_end.month_date
      AND bamboohr_base.field_name = headcount_end.breakout_type
      AND bamboohr_base.field_value = headcount_end.division_department
    WHERE bamboohr_base.field_name = 'division_grouping_breakout'
      AND promotions.division_grouping = 'Marketing'
      AND promotions.department != 'Sales Development'

    UNION ALL

    SELECT
      bamboohr_base.*,
      promotions.*,
      headcount_end,
      rolling_12_month_headcount
    FROM bamboohr_base
    LEFT JOIN promotions
      ON promotions.promotion_month BETWEEN rolling_start_month AND rolling_end_month
    LEFT JOIN headcount_end
      ON bamboohr_base.rolling_end_month = headcount_end.month_date
      AND bamboohr_base.field_name = headcount_end.breakout_type
      AND bamboohr_base.field_value = headcount_end.division_department
    WHERE bamboohr_base.field_name = 'company_breakout'

    UNION ALL

    SELECT 
      bamboohr_base.rolling_start_month,
      bamboohr_base.rolling_end_month,
      'company_breakout' AS field_name,
      'Company - Excluding SDR'  AS field_value,
      promotions.*,
      headcount_end_excluding_sdr,
      rolling_12_month_headcount_excluding_sdr
    FROM bamboohr_base
    LEFT JOIN promotions
      ON promotions.promotion_month BETWEEN rolling_start_month AND rolling_end_month
    LEFT JOIN headcount_end
      ON bamboohr_base.rolling_end_month = headcount_end.month_date
      AND bamboohr_base.field_name = headcount_end.breakout_type
      AND bamboohr_base.field_value = headcount_end.division_department
    WHERE bamboohr_base.field_name = 'company_breakout'
      AND promotions.department != 'Sales Development'

), final AS (

    SELECT
      rolling_end_month                                         AS month_date,
      field_name, 
      field_value,
      headcount_end, 
      rolling_12_month_headcount,
      COUNT(employee_id)                                        AS total_promotions,
        total_promotions/NULLIFZERO(rolling_12_month_headcount) AS promotion_rate,
      IFF(total_promotions <= 3, NULL, 
            AVG(percent_change_in_comp))                        AS average_percent_change_in_comp,
      IFF(total_promotions <= 3, NULL, 
            MEDIAN(percent_change_in_comp))                     AS median_percent_change_change_in_comp
    FROM joined
    GROUP BY 1,2,3,4,5

)

SELECT *
FROM final
