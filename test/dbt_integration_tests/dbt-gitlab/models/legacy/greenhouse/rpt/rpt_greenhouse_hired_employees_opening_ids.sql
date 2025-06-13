WITH employees AS (

    SELECT * 
    FROM {{ref('employee_directory')}}

), greenhouse_applications AS (

    SELECT * 
    FROM {{ref('greenhouse_applications_source')}}

), greenhouse_openings AS (

    SELECT * 
    FROM {{ref('greenhouse_openings_source')}}

), greenhouse_jobs AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs_source')}}

 ), bamboohr_job_info AS (

    SELECT *
    FROM {{ ref('bamboohr_job_info') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date) = 1

), aggregated AS (

    SELECT
        opening_id,
        job_name                                                                AS job_opening_name,
        greenhouse_jobs.job_opened_at, 
        CONCAT(first_name,' ', last_name)                                       AS full_name,     
        department                                                              AS department_hired_into, 
        division                                                                AS division_hired_into, 
        job_title                                                               AS job_hired_into     
    FROM employees
    INNER JOIN greenhouse_applications
      ON employees.greenhouse_candidate_id = greenhouse_applications.candidate_id 
    INNER JOIN greenhouse_openings
      ON greenhouse_openings.hired_application_id = greenhouse_applications.application_id
    INNER JOIN greenhouse_jobs 
      ON greenhouse_jobs.job_id = greenhouse_openings.job_id
    INNER JOIN bamboohr_job_info 
      ON bamboohr_job_info.employee_id = employees.employee_id 
    WHERE greenhouse_candidate_id IS NOT NULL 

)

SELECT * 
FROM aggregated
