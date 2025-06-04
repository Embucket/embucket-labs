{% set partition_statement = "OVER (PARTITION BY base.breakout_type, base.department, base.division, base.job_role,
                                    base.job_grade, base.eeoc_field_name, base.eeoc_value
                              ORDER BY base.month_date DESC ROWS BETWEEN CURRENT ROW AND 11 FOLLOWING)
                              " %}


{% set ratio_to_report_partition_statement = "OVER (PARTITION BY base.month_date, base.breakout_type, base.department, base.division, base.job_role,
                                              base.job_grade, base.eeoc_field_name
                                              ORDER BY base.month_date)
                              " %}

WITH source AS (

    SELECT *
    FROM {{ ref ('bamboohr_headcount_intermediate') }}

), base AS (

    SELECT DISTINCT 
      unique_key,
      month_date,
      breakout_type_modified AS breakout_type,
      department,
      division,
      job_role,
      job_grade,
      eeoc_field_name,                                                       
      eeoc_value
      --- this is to group groups with less than 4 headcount
    FROM source

), intermediate AS (

   SELECT
      base.month_date,
      IFF(base.month_date = DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE())), 
                TRUE, FALSE)                                                       AS is_last_month,
      base.breakout_type, 
      base.department,
      base.division,
      base.job_role,
      base.job_grade,
      base.eeoc_field_name,
      base.eeoc_value,
      IFF(base.breakout_type !='eeoc_breakout' 
            AND base.eeoc_field_name !='no_eeoc', FALSE, TRUE)                     AS show_value_criteria,
      headcount_start,
      headcount_start_excluding_sdr,
      headcount_end,
      headcount_end_excluding_sdr,
      headcount_average,
      headcount_average_excluding_sdr,
      hire_count,
      separation_count,
      voluntary_separation,
      involuntary_separation,
      AVG(COALESCE(headcount_average, 0)) {{partition_statement}}                   AS rolling_12_month_headcount,
      AVG(COALESCE(headcount_average_excluding_sdr, 0)) {{partition_statement}}     AS rolling_12_month_headcount_excluding_sdr,
      SUM(COALESCE(separation_count,0)) {{partition_statement}}                     AS rolling_12_month_separations,
      SUM(COALESCE(voluntary_separation,0)) {{partition_statement}}                 AS rolling_12_month_voluntary_separations,
      SUM(COALESCE(involuntary_separation,0)) {{partition_statement}}               AS rolling_12_month_involuntary_separations,
      IFF(rolling_12_month_headcount< rolling_12_month_separations, NULL,
        1 - (rolling_12_month_separations/NULLIF(rolling_12_month_headcount,0)))    AS retention,

      headcount_end_leader,
      headcount_average_leader,
      hired_leaders,
      separated_leaders,
      AVG(COALESCE(headcount_average_leader, 0)) {{partition_statement}}             AS rolling_12_month_headcount_leader,
      SUM(COALESCE(separated_leaders,0)) {{partition_statement}}                     AS rolling_12_month_separations_leader,
      IFF(rolling_12_month_headcount_leader< rolling_12_month_separations_leader, NULL,
        1 - (rolling_12_month_separations_leader/NULLIF(rolling_12_month_headcount_leader,0)))    AS retention_leader,

      headcount_end_manager,
      headcount_average_manager,
      hired_manager,
      separated_manager,
      AVG(COALESCE(headcount_average_manager, 0)) {{partition_statement}}             AS rolling_12_month_headcount_manager,
      SUM(COALESCE(separated_manager,0)) {{partition_statement}}                      AS rolling_12_month_separations_manager,
      IFF(rolling_12_month_headcount_manager< rolling_12_month_separations_manager, NULL,
        1 - (rolling_12_month_separations_manager/NULLIF(rolling_12_month_headcount_manager,0)))    AS retention_manager,

      headcount_end_management,
      headcount_average_management,
      hired_management,
      separated_management,
      AVG(COALESCE(headcount_average_management, 0)) {{partition_statement}}             AS rolling_12_month_headcount_management,
      SUM(COALESCE(separated_management,0)) {{partition_statement}}                      AS rolling_12_month_separations_management,
      IFF(rolling_12_month_headcount_management< rolling_12_month_separations_management, 
        NULL,
        1 - (rolling_12_month_separations_management/
            NULLIF(rolling_12_month_headcount_management,0)))                            AS retention_management,

      headcount_end_individual_contributor,
      headcount_average_contributor,
      hired_contributor,
      separated_contributor,
      
      MIN(headcount_end_individual_contributor)
        {{ratio_to_report_partition_statement}}                                     AS min_headcount_end_contributor, 
      SUM(headcount_end_individual_contributor)
        {{ratio_to_report_partition_statement}}                                     AS total_headcount_end_contributor,   
      MIN(headcount_average)  {{ratio_to_report_partition_statement}}               AS min_headcount_average,
      SUM(headcount_end)  {{ratio_to_report_partition_statement}}                   AS total_headcount_end,
      MIN(hire_count) {{ratio_to_report_partition_statement}}                       AS min_hire_count,
      SUM(hire_count) {{ratio_to_report_partition_statement}}                       AS total_hire_count,
      MIN(headcount_average_leader) {{ratio_to_report_partition_statement}}         AS min_headcount_leader,
      SUM(headcount_average_leader) {{ratio_to_report_partition_statement}}         AS total_headcount_leader,
      MIN(headcount_average_manager) {{ratio_to_report_partition_statement}}        AS min_headcount_manager,
      SUM(headcount_average_manager) {{ratio_to_report_partition_statement}}        AS total_headcount_manager,
      MIN(headcount_average_management) {{ratio_to_report_partition_statement}}     AS min_headcount_management,
      SUM(headcount_average_management) {{ratio_to_report_partition_statement}}     AS total_headcount_management,
      MIN(headcount_average_contributor) {{ratio_to_report_partition_statement}}    AS min_headcount_contributor,


      RATIO_TO_REPORT(headcount_end) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount,
      RATIO_TO_REPORT(hire_count) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_hires,
      RATIO_TO_REPORT(headcount_end_leader) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_leaders,
      RATIO_TO_REPORT(headcount_end_manager) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_manager,    
      RATIO_TO_REPORT(headcount_end_management) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_management,          
      RATIO_TO_REPORT(headcount_end_individual_contributor) 
        {{ratio_to_report_partition_statement}}                                     AS percent_of_headcount_contributor,
      
      SUM(COALESCE(promotion,0)) {{partition_statement}}                            AS rolling_12_month_promotions,
      SUM(COALESCE(promotion_excluding_sdr,0)) {{partition_statement}}              AS rolling_12_month_promotions_excluding_sdr,

      SUM(COALESCE(percent_change_in_comp,0)) {{partition_statement}}               AS rolling_12_month_promotions_percent_change_in_comp,
      SUM(COALESCE(percent_change_in_comp_excluding_sdr,0)) {{partition_statement}} AS rolling_12_month_promotions_percent_change_in_comp_excluding_sdr,
      location_factor,
      AVG(new_hire_location_factor)
        OVER (PARTITION BY base.breakout_type, base.department, base.division, base.job_role,
                           base.job_grade, base.eeoc_field_name, base.eeoc_value
              ORDER BY base.month_date DESC ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS new_hire_location_factor_rolling_3_month,
      discretionary_bonus,
      tenure_months,
      tenure_zero_to_six_months,
      tenure_six_to_twelve_months,
      tenure_one_to_two_years,
      tenure_two_to_four_years,
      tenure_four_plus_years
    FROM base
    LEFT JOIN source  
      ON base.unique_key = source.unique_key
    WHERE base.month_date < DATE_TRUNC('month', CURRENT_DATE)   

 ), final AS (
     
    SELECT   
      month_date,
      is_last_month,
      breakout_type, 
      department,
      division,
      job_role,
      job_grade,
      eeoc_field_name,
      CASE WHEN eeoc_field_name = 'gender' AND headcount_end < 5
            THEN 'Other'
           WHEN eeoc_field_name = 'gender-region' AND headcount_end < 5
            THEN 'Other_'|| SPLIT_PART(eeoc_value,'_',2)
           WHEN eeoc_field_name = 'ethnicity' AND headcount_end < 5
            THEN 'Other'       
           ELSE eeoc_value END                                                 AS eeoc_value,
      IFF(headcount_start <4 AND show_value_criteria = FALSE,
        NULL,headcount_start)                                                  AS headcount_start,
      IFF(headcount_start_excluding_sdr <4 AND show_value_criteria = FALSE,
        NULL,headcount_start_excluding_sdr)                                    AS headcount_start_excluding_sdr,  
      IFF(headcount_end <4 AND show_value_criteria = FALSE,
        NULL, headcount_end)                                                   AS headcount_end,
      IFF(headcount_end_excluding_sdr <4 AND show_value_criteria = FALSE,
        NULL, headcount_end_excluding_sdr)                                     AS headcount_end_excluding_sdr,      
      IFF(headcount_average <4 AND eeoc_field_name != 'no_eeoc',  
        NULL, headcount_average)                                               AS headcount_average,
      IFF(headcount_average_excluding_sdr <4 AND eeoc_field_name != 'no_eeoc',  
        NULL, headcount_average)                                               AS headcount_average_excluding_sdr,  
      IFF(hire_count <4 AND eeoc_field_name != 'no_eeoc', 
        NULL, hire_count)                                                      AS hire_count,
      IFF(separation_count <4 AND eeoc_field_name != 'no_eeoc', 
        NULL, separation_count)                                                AS separation_count,
      IFF(voluntary_separation <4, NULL, voluntary_separation)                 AS voluntary_separation_count,
      IFF(voluntary_separation < 4,  NULL, involuntary_separation)             AS involuntary_separation_count,  

      rolling_12_month_headcount,
      rolling_12_month_headcount_excluding_sdr,
      rolling_12_month_separations,
      rolling_12_month_voluntary_separations,
      rolling_12_month_involuntary_separations,
      IFF(rolling_12_month_headcount< rolling_12_month_voluntary_separations, NULL,
        (rolling_12_month_voluntary_separations/NULLIF(rolling_12_month_headcount,0)))    AS voluntary_separation_rate,
      IFF(rolling_12_month_headcount< rolling_12_month_involuntary_separations, NULL,
        (rolling_12_month_involuntary_separations/NULLIF(rolling_12_month_headcount,0)))  AS involuntary_separation_rate,
      retention,

      IFF(headcount_end_leader < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_leader)                                         AS headcount_end_leader, 
      IFF(headcount_average_leader < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL,headcount_average_leader)                                      AS headcount_leader_average,
      IFF(hired_leaders < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_leaders)                                                AS hired_leaders,
      IFF(separated_leaders < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, separated_leaders)                                            AS separated_leaders,
      rolling_12_month_headcount_leader,
      rolling_12_month_separations_leader,
      retention_leader,


      IFF(headcount_end_manager < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_manager)                                        AS headcount_end_manager,            
      IFF(headcount_average_manager < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_average_manager)                                    AS headcount_manager_average,
      IFF(hired_manager < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_manager)                                                AS hired_manager,
      IFF(separated_manager < 2 AND eeoc_field_name != 'no_eeoc',
        NULL, separated_manager)                                            AS separated_manager,
      rolling_12_month_headcount_manager,
      rolling_12_month_separations_manager,
      retention_manager,
 
      IFF(headcount_end_management < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_management)                                        AS headcount_end_management,            
      IFF(headcount_average_management < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_average_management)                                    AS headcount_management_average,
      IFF(hired_management < 2 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_management)                                                AS hired_management,
      IFF(separated_management < 2 AND eeoc_field_name != 'no_eeoc',
        NULL, separated_management)                                            AS separated_management,
      rolling_12_month_headcount_management,
      rolling_12_month_separations_management,
      retention_management,

      IFF(headcount_end_individual_contributor < 4 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_end_individual_contributor)                              AS headcount_end_contributor,
      IFF(headcount_average_contributor < 4 AND eeoc_field_name != 'no_eeoc', 
        NULL, headcount_average_contributor)                                     AS headcount_contributor,
      IFF(hired_contributor < 4 AND eeoc_field_name != 'no_eeoc', 
        NULL, hired_contributor)                                                 AS hired_contributor,
      IFF(separated_contributor < 4 AND eeoc_field_name != 'no_eeoc',
        NULL, separated_contributor)                                             AS separated_contributor,

      IFF(total_headcount_end < 5 AND show_value_criteria = FALSE, 
            NULL, percent_of_headcount)                                          AS percent_of_headcount,
      IFF(total_hire_count < 5 AND show_value_criteria = FALSE, 
        NULL, percent_of_hires)                                                  AS percent_of_hires,
      IFF(total_headcount_leader < 3 AND show_value_criteria = FALSE, 
        NULL, percent_of_headcount_leaders)                                      AS percent_of_headcount_leaders,
      IFF(total_headcount_manager < 3 AND show_value_criteria = FALSE,  
        NULL, percent_of_headcount_manager)                                      AS percent_of_headcount_manager,
      IFF(total_headcount_management < 3 AND show_value_criteria = FALSE,  
        NULL, percent_of_headcount_management)                                   AS percent_of_headcount_management,
      IFF(total_headcount_end_contributor < 5 AND show_value_criteria = FALSE, 
        NULL, percent_of_headcount_contributor)                                  AS percent_of_headcount_contributor,

      CASE WHEN breakout_type IN ('kpi_breakout','division_breakout','department_breakout') 
            AND eeoc_value = 'no_eeoc'
            THEN rolling_12_month_promotions
           WHEN breakout_type IN ('eeoc_breakout')
             AND eeoc_field_name IN ('gender','ethnicity','region_modified')
             AND rolling_12_month_promotions > 3
            THEN rolling_12_month_promotions
            ELSE NULL END                                                         AS rolling_12_month_promotions,   
            
      CASE WHEN breakout_type IN ('kpi_breakout','division_breakout','department_breakout') 
            AND eeoc_value = 'no_eeoc'
            THEN rolling_12_month_promotions_excluding_sdr
           WHEN breakout_type IN ('eeoc_breakout')
             AND eeoc_field_name IN ('gender','ethnicity','region_modified')
             AND rolling_12_month_promotions > 3
            THEN rolling_12_month_promotions_excluding_sdr
            ELSE NULL END                                                         AS rolling_12_month_promotions_excluding_sdr,
      CASE 
        WHEN breakout_type IN ('kpi_breakout','division_breakout','department_breakout') 
            AND eeoc_value = 'no_eeoc'
            AND rolling_12_month_promotions > 3
        THEN rolling_12_month_promotions_percent_change_in_comp/rolling_12_month_promotions
        WHEN breakout_type IN ('eeoc_breakout') 
            AND eeoc_field_name IN ('gender','ethnicity','region_modified')
            AND rolling_12_month_promotions > 3
          THEN rolling_12_month_promotions_percent_change_in_comp/rolling_12_month_promotions
        ELSE NULL END                                                            AS rolling_12_month_promotion_increase,
      CASE 
        WHEN breakout_type IN ('kpi_breakout','division_breakout','department_breakout') 
            AND eeoc_value = 'no_eeoc'
            AND rolling_12_month_promotions_excluding_sdr > 3
        THEN rolling_12_month_promotions_percent_change_in_comp_excluding_sdr/rolling_12_month_promotions_excluding_sdr
        WHEN breakout_type IN ('eeoc_breakout') 
            AND eeoc_field_name IN ('gender','ethnicity','region_modified')
            AND rolling_12_month_promotions_excluding_sdr > 3
          THEN rolling_12_month_promotions_percent_change_in_comp_excluding_sdr/rolling_12_month_promotions_excluding_sdr
        ELSE NULL END                                                            AS rolling_12_month_promotion_increase_excluding_sdr,

      IFF(headcount_end <4 AND show_value_criteria = FALSE,
        NULL,location_factor)                                                    AS location_factor,

      new_hire_location_factor_rolling_3_month,        
      IFF(discretionary_bonus<4 AND show_value_criteria = FALSE,
        NULL, discretionary_bonus)                                               AS discretionary_bonus,
      IFF(tenure_months<4 AND show_value_criteria = FALSE,
        NULL, tenure_months)                                                     AS tenure_months,
      IFF(tenure_zero_to_six_months<4 AND show_value_criteria  = FALSE,
        NULL, tenure_zero_to_six_months)                                         AS tenure_zero_to_six_months,
      IFF(tenure_six_to_twelve_months<4 AND show_value_criteria = FALSE,
        NULL, tenure_six_to_twelve_months)                                       AS tenure_six_to_twelve_months,
      IFF(tenure_one_to_two_years<4 AND show_value_criteria = FALSE,
        NULL, tenure_one_to_two_years)                                           AS tenure_one_to_two_years,
      IFF(tenure_two_to_four_years<4 AND show_value_criteria = FALSE,
        NULL, tenure_two_to_four_years)                                          AS tenure_two_to_four_years,
      IFF(tenure_four_plus_years<4 AND show_value_criteria = FALSE,
        NULL, tenure_four_plus_years)                                            AS tenure_four_plus_years
    FROM intermediate   

)

SELECT * 
FROM final
