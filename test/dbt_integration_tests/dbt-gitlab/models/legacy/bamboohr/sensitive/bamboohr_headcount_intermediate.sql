{{ config({
    "materialized":"table",
    })
}}

{% set repeated_metric_columns = 
      "SUM(headcount_start)                                                         AS headcount_start,
      SUM(headcount_start_excluding_sdr)                                            AS headcount_start_excluding_sdr,
      SUM(headcount_end)                                                            AS headcount_end,
      SUM(headcount_end_excluding_sdr)                                              AS headcount_end_excluding_sdr,
      (SUM(headcount_start) + SUM(headcount_end))/2                                 AS headcount_average,
      (SUM(headcount_start_excluding_sdr) + SUM(headcount_end_excluding_sdr))/2     AS headcount_average_excluding_sdr,
      SUM(hire_count)                                                               AS hire_count,
      SUM(separation_count)                                                         AS separation_count,
      SUM(voluntary_separation)                                                     AS voluntary_separation,
      SUM(involuntary_separation)                                                   AS involuntary_separation,

      SUM(headcount_start_leader)                                                   AS headcount_start_leader,
      SUM(headcount_end_leader)                                                     AS headcount_end_leader,
      (SUM(headcount_start_leader) 
        + SUM(headcount_end_leader))/2                                              AS headcount_average_leader,
      SUM(hired_leaders)                                                            AS hired_leaders,
      SUM(separated_leaders)                                                        AS separated_leaders,

      SUM(headcount_start_manager)                                                  AS headcount_start_manager,
      SUM(headcount_end_manager)                                                    AS headcount_end_manager,
      (SUM(headcount_start_manager) 
        + SUM(headcount_end_leader))/2                                              AS headcount_average_manager,
      SUM(hired_manager)                                                            AS hired_manager,
      SUM(separated_manager)                                                        AS separated_manager,

      SUM(headcount_start_management)                                               AS headcount_start_management,
      SUM(headcount_end_management)                                                 AS headcount_end_management,
      (SUM(headcount_start_management) 
        + SUM(headcount_end_management))/2                                          AS headcount_average_management,
      SUM(hired_management)                                                         AS hired_management,
      SUM(separated_management)                                                     AS separated_management,
            
      SUM(headcount_start_contributor)                                              AS headcount_start_contributor,
      SUM(headcount_end_contributor)                                                AS headcount_end_individual_contributor,
      (SUM(headcount_start_contributor) 
        + SUM(headcount_end_contributor))/2                                         AS headcount_average_contributor,
      SUM(hired_contributor)                                                        AS hired_contributor,
      SUM(separated_contributor)                                                    AS separated_contributor,

      SUM(IFF(is_promotion = TRUE,1,0))                                             AS promotion,
      SUM(IFF(is_promotion_excluding_sdr = TRUE,1,0))                               AS promotion_excluding_sdr,
      
      SUM(percent_change_in_comp)                                                   AS percent_change_in_comp,
      SUM(percent_change_in_comp_excluding_sdr)                                     AS percent_change_in_comp_excluding_sdr,

      AVG(location_factor)                                                          AS location_factor,
      AVG(new_hire_location_factor)                                                 AS new_hire_location_factor,
      SUM(discretionary_bonus)                                                      AS discretionary_bonus, 
      AVG(tenure_months)                                                            AS tenure_months,
      SUM(tenure_zero_to_six_months)                                                AS tenure_zero_to_six_months,
      SUM(tenure_six_to_twelve_months)                                              AS tenure_six_to_twelve_months,
      SUM(tenure_one_to_two_years)                                                  AS tenure_one_to_two_years,
      SUM(tenure_two_to_four_years)                                                 AS tenure_two_to_four_years,
      SUM(tenure_four_plus_years)                                                   AS tenure_four_plus_years
      "%}



WITH dates AS (

    SELECT
      date_actual                                 AS start_date,
      LAST_DAY(date_actual)                       AS end_date
    FROM {{ ref ('date_details') }}
    WHERE date_day <= LAST_DAY(current_date)
       AND day_of_month = 1
       AND date_actual >= '2013-07-01' -- min employment_status_date in bamboohr_employment_status model

), mapping AS (

    {{ dbt_utils.unpivot(relation=ref('bamboohr_id_employee_number_mapping'), cast_to='varchar', 
       exclude=['employee_number', 'employee_id','first_name', 'last_name', 'hire_date', 'termination_date', 'greenhouse_candidate_id','region','country','nationality', 'last_updated_date', 'first_inactive_date']) }}

), mapping_enhanced AS (

    SELECT 
      employee_id,
      LOWER(field_name)                  AS eeoc_field_name, 
      COALESCE(value, 'Not Identified')  AS eeoc_value
    FROM mapping

    UNION ALL

    SELECT 
      DISTINCT employee_id,
      'no_eeoc'                         AS eeoc_field_name,
      'no_eeoc'                         AS eeoc_value
    FROM mapping
 
), separation_reason AS(

    SELECT * 
    FROM {{ ref ('bamboohr_employment_status_xf') }}
    WHERE employment_status = 'Terminated'

), employees AS (

    SELECT *
    FROM {{ ref ('employee_directory_intermediate') }}

), bamboohr_promotion AS (

    SELECT *
    FROM {{ ref ('bamboohr_promotions_xf') }}

), intermediate AS (

    SELECT
      employees.date_actual,
      employees.department_modified                                                 AS department,
      division_mapped_current                                                       AS division,
      --using the current division - department mapping for reporting
      job_role_modified                                                             AS job_role,
      COALESCE(job_grade,'NA')                                                      AS job_grade,
      mapping_enhanced.eeoc_field_name,
      CASE
        WHEN mapping_enhanced.eeoc_field_name like '%_region'
          THEN SPLIT_PART(mapping_enhanced.eeoc_value,'_',1)|| '_' || 
            IFF(employees.country in ('United States of America', 'United States'),'United States of America', 'Non-US')
        WHEN mapping_enhanced.eeoc_field_name = 'region_modified'
          THEN employees.region_modified
        ELSE mapping_enhanced.eeoc_value END                                        AS eeoc_value,   
      IFF(dates.start_date = date_actual,1,0)                                       AS headcount_start,
      IFF(dates.start_date = date_actual 
        AND employees.department_modified != 'Sales Development', 1,0)              AS headcount_start_excluding_sdr,
      IFF(dates.end_date = date_actual,1,0)                                         AS headcount_end,
      IFF(dates.end_date = date_actual 
        AND employees.department_modified != 'Sales Development', 1,0)              AS headcount_end_excluding_sdr,
      IFF(is_hire_date = True, 1,0)                                                 AS hire_count,
      IFF(termination_type = 'Resignation (Voluntary)',1,0)                         AS voluntary_separation,
      IFF(termination_type = 'Termination (Involuntary)',1,0)                       AS involuntary_separation,
      voluntary_separation + involuntary_separation                                 AS separation_count,

      IFF(dates.start_date = date_actual 
          AND job_role_modified = 'Director+',1,0)                          AS headcount_start_leader,
      IFF(dates.end_date = date_actual
          AND job_role_modified = 'Director+',1,0)                          AS headcount_end_leader,
      IFF(is_hire_date = True 
          AND job_role_modified = 'Director+',1,0)                          AS hired_leaders,
      IFF(is_termination_date = True
          AND job_role_modified = 'Director+',1,0)                          AS separated_leaders,
      
      IFF(dates.start_date = date_actual 
          AND job_role_modified = 'Manager',1,0)                                    AS headcount_start_manager,
      IFF(dates.end_date = date_actual
          AND job_role_modified = 'Manager',1,0)                                    AS headcount_end_manager,
      IFF(is_hire_date = True 
          AND job_role_modified = 'Manager',1,0)                                    AS hired_manager,
      IFF(is_termination_date = True
          AND job_role_modified = 'Manager',1,0)                                    AS separated_manager,

      IFF(dates.start_date = date_actual 
          AND job_role_modified != 'Individual Contributor',1,0)                    AS headcount_start_management,
      IFF(dates.end_date = date_actual
          AND job_role_modified != 'Individual Contributor',1,0)                    AS headcount_end_management,
      IFF(is_hire_date = True 
          AND job_role_modified != 'Individual Contributor',1,0)                    AS hired_management,
      IFF(is_termination_date = True
          AND job_role_modified != 'Individual Contributor',1,0)                    AS separated_management,   

       IFF(dates.start_date = date_actual 
          AND job_role_modified = 'Individual Contributor',1,0)                     AS headcount_start_contributor,
      IFF(dates.end_date = date_actual
          AND job_role_modified = 'Individual Contributor',1,0)                     AS headcount_end_contributor,
      IFF(is_hire_date = True 
          AND job_role_modified = 'Individual Contributor',1,0)                     AS hired_contributor,
      IFF(is_termination_date = True
          AND job_role_modified = 'Individual Contributor',1,0)                     AS separated_contributor, 


      IFF(employees.job_title LIKE '%VP%', 'Exclude', is_promotion)                 AS is_promotion,  
      IFF(employees.job_title LIKE '%VP%' 
        OR employees.department_modified = 'Sales Development',
        'Exclude',is_promotion)                                                     AS is_promotion_excluding_sdr,
      IFF(is_promotion = TRUE AND employees.job_title NOT LIKE '%VP%',
        percent_change_in_comp, NULL)                                               AS percent_change_in_comp,      
      IFF(employees.job_title LIKE '%VP%' 
        OR employees.department_modified = 'Sales Development',
        NULL,percent_change_in_comp)                                                AS percent_change_in_comp_excluding_sdr,

      IFF(dates.end_date = date_actual 
            AND COALESCE(sales_geo_differential,'n/a - Comp Calc') = 'n/a - Comp Calc',
            location_factor, NULL)                                                  AS location_factor,
      IFF(is_hire_date = TRUE
            AND COALESCE(sales_geo_differential,'n/a - Comp Calc') = 'n/a - Comp Calc',
            location_factor, NULL)                                                  AS new_hire_location_factor,      
      discretionary_bonus,      
      ROUND((tenure_days/30),2)                                                     AS tenure_months,
      IFF(tenure_months BETWEEN 0 AND 6 AND dates.end_date = date_actual, 1, 0)     AS tenure_zero_to_six_months,
      IFF(tenure_months BETWEEN 6 AND 12 AND dates.end_date = date_actual, 1, 0)    AS tenure_six_to_twelve_months,
      IFF(tenure_months BETWEEN 12 AND 24 AND dates.end_date = date_actual, 1, 0)   AS tenure_one_to_two_years,
      IFF(tenure_months BETWEEN 24 AND 48 AND dates.end_date = date_actual, 1, 0)   AS tenure_two_to_four_years,
      IFF(tenure_months >= 48 AND dates.end_date = date_actual, 1, 0)               AS tenure_four_plus_years
    FROM dates
    LEFT JOIN employees
      ON DATE_TRUNC(month,dates.start_date) = DATE_TRUNC(month, employees.date_actual)
    LEFT JOIN mapping_enhanced
      ON employees.employee_id = mapping_enhanced.employee_id
    LEFT JOIN separation_reason
      ON separation_reason.employee_id = employees.employee_id
      AND employees.date_actual = separation_reason.valid_from_date
    LEFT JOIN bamboohr_promotion
      ON employees.employee_id = bamboohr_promotion.employee_id
      AND employees.date_actual = bamboohr_promotion.promotion_date  
   WHERE date_actual IS NOT NULL


), aggregated AS (

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'all_attributes_breakout'         AS breakout_type,
      department,
      division,
      job_role,
      job_grade,
      eeoc_field_name,                                                       
      eeoc_value,    
     {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    {{ dbt_utils.group_by(n=8) }}  


    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'department_breakout'             AS breakout_type,
      department,
      division,
      NULL                              AS job_role,
      NULL                              AS job_grade,
      eeoc_field_name,                                                       
      eeoc_value,    
     {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    {{ dbt_utils.group_by(n=8) }}  

    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'eeoc_breakout'                   AS breakout_type, 
      'eeoc_breakout'                   AS department,
      'eeoc_breakout'                   AS division,
      NULL                              AS job_role,
      NULL                              AS job_grade,
      eeoc_field_name,                                                       
      eeoc_value,  
      {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    {{ dbt_utils.group_by(n=8) }} 

    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'division_breakout'               AS breakout_type, 
      'division_breakout'               AS department,
      division,
      NULL                              AS job_role,
      NULL                              AS job_grade,
      eeoc_field_name,                                                       
      eeoc_value,
      {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    WHERE department IS NOT NULL
    {{ dbt_utils.group_by(n=8) }} 

    UNION ALL

    SELECT
      DATE_TRUNC(month,start_date)      AS month_date,
      'division_group_breakout'         AS breakout_type, 
      'division_group_breakout'         AS department,
      CASE
        WHEN division = 'Sales' 
          THEN 'Sales'
        WHEN division in ('Product', 'Engineering', 'Security')
          THEN 'Tech'
        ELSE 'Non-Tech' END             AS division,
      NULL                              AS job_role,
      NULL                              AS job_grade,
      eeoc_field_name,                                                       
      eeoc_value,
      {{repeated_metric_columns}}
    FROM dates 
    LEFT JOIN intermediate 
      ON DATE_TRUNC(month, start_date) = DATE_TRUNC(month, date_actual)
    WHERE department IS NOT NULL
    {{ dbt_utils.group_by(n=8) }} 

), breakout_modified AS (

    SELECT 
      aggregated.*,
      IFF(breakout_type = 'eeoc_breakout'
          AND eeoc_field_name = 'no_eeoc', 'kpi_breakout', breakout_type)                   AS breakout_type_modified
    FROM aggregated

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['month_date', 'breakout_type_modified','department','division',
                                'job_role','job_grade', 'eeoc_field_name', 'eeoc_value']) }} AS unique_key,
      breakout_modified.*                           
    FROM breakout_modified

)

SELECT *
FROM final
