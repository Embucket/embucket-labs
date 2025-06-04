WITH dates AS (

    SELECT *,
      'join' AS join_field
    FROM  {{ ref('dim_date') }}
    WHERE date_actual BETWEEN DATEADD(YEAR, -1, DATEADD(month, -1, DATE_TRUNC(month, CURRENT_DATE())))
                          AND DATE_TRUNC(month, CURRENT_DATE())
      AND day_of_month = 1

), division_department_mapping AS (

    SELECT *,
      'join' AS join_field
    FROM  {{ ref('bamboohr_job_info_current_division_base') }}
    WHERE DATE_TRUNC(month, CURRENT_DATE()) BETWEEN effective_date AND COALESCE(effective_end_date, termination_date,CURRENT_DATE())


), unioned AS (

    SELECT 
      DISTINCT 
      dates.date_actual,
      'division_grouping_breakout'                   AS field_name,
      division_grouping                              AS field_value
    FROM dates
    LEFT JOIN division_department_mapping
      ON dates.join_field = division_department_mapping.join_field
    
    UNION ALL

    SELECT 
      DISTINCT 
      dates.date_actual,
      'department_grouping_breakout'          AS field_name,
      department_grouping                     AS field_value
    FROM dates
    LEFT JOIN division_department_mapping
      ON dates.join_field = division_department_mapping.join_field

    UNION ALL

    SELECT
      dates.date_actual,
      'company_breakout'                    AS field_name,
      'company_breakout'                    AS field_value
    FROM dates     

)

SELECT *
FROM unioned
