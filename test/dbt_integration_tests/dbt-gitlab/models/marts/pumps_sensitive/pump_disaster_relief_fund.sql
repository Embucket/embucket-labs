WITH current_employees AS (

    SELECT *
    FROM {{ ref('employee_directory') }}

), hires AS (

    SELECT 
      first_name,
      last_name,
      employee_number,
      last_work_email,
      'A' AS action,
      hire_date as event_date
    FROM current_employees
    WHERE hire_date IS NOT NULL

), terminations AS (

    SELECT 
      first_name,
      last_name,
      employee_number,
      last_work_email,
      'D' AS action,
      termination_date as event_date
    FROM current_employees
    WHERE termination_date IS NOT NULL

), unioned AS (

    SELECT *
    FROM hires

    UNION ALL

    SELECT *
    FROM terminations

), report_table AS (

    SELECT 
      '3936'          AS "Fund",
      first_name      AS "Firstname",
      last_name       AS "Lastname",
      employee_number AS "Employeenumber",
      last_work_email AS "Email",
      action          AS "Action",
      CURRENT_DATE()  AS report_date
    FROM unioned
    WHERE  (CASE
             WHEN DAYOFMONTH(report_date) <= 15
               AND DAYOFMONTH(event_date) > 15
               AND DATE_TRUNC('month', DATEADD('month', -1, report_date)) = DATE_TRUNC('month', event_date) 
             THEN TRUE
             WHEN DAYOFMONTH(report_date) > 15
               AND DAYOFMONTH(event_date) <= 15
               AND DATE_TRUNC('month', report_date) = DATE_TRUNC('month', event_date) 
             THEN TRUE
             ELSE FALSE
           END) = TRUE

)

SELECT *
FROM report_table