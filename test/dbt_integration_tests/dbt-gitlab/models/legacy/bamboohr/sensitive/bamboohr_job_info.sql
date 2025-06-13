WITH source AS (
  SELECT * 
  FROM {{ ref('blended_job_info_source') }}
),

employee_name AS (
  SELECT * 
  FROM {{ ref('blended_directory_source') }}
  QUALIFY ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY uploaded_at DESC) = 1
  AND ROW_NUMBER() OVER(PARTITION BY full_name ORDER BY uploaded_at DESC) = 1
),

bamboohr_employment_status AS (
  
    SELECT
      employee_id,
      valid_from_date,
      DATEADD(day,1,valid_from_date) AS valid_to_date ---adding a day to capture termination date
    FROM {{ ref('bamboohr_employment_status_xf') }}  
    WHERE employment_status = 'Terminated'

),

sheetload_job_roles AS (

    SELECT *
    FROM {{ source('sheetload', 'job_roles_prior_to_2020_02') }}

),

cleaned AS (

    SELECT 
      job_sequence,
      source.employee_id,
      source.job_title,
      source.effective_date, --the below case when statement is also used in employee_directory_analysis until we upgrade to 0.14.0 of dbt
      employee_name.employee_id AS reports_to_id,
      CASE WHEN division = 'Alliances' THEN 'Alliances'
           WHEN division = 'Customer Support' THEN 'Customer Support'
           WHEN division = 'Customer Service' THEN 'Customer Success'
           WHEN department = 'Data & Analytics' THEN 'Business Operations'
           ELSE NULLIF(department, '') END                                      AS department,
      CASE WHEN department = 'Meltano' THEN 'Meltano'
           WHEN division = 'Employee' THEN null
           WHEN division = 'Contractor ' THEN null
           WHEN division = 'Alliances' Then 'Sales'
           WHEN division = 'Customer Support' THEN 'Engineering'
           WHEN division = 'Customer Service' THEN 'Sales'
           ELSE NULLIF(division, '') END                                        AS division,
      entity,
      reports_to,
      (LAG(DATEADD('day',-1,source.effective_date), 1) OVER (PARTITION BY source.employee_id ORDER BY source.effective_date DESC, job_sequence DESC)) AS effective_end_date
    FROM source
    LEFT JOIN employee_name
      ON employee_name.full_name = source.reports_to

    
),

joined AS (

    SELECT 
      cleaned.job_sequence,
      cleaned.employee_id,
      cleaned.job_title,
      cleaned.reports_to_id,
      cleaned.effective_date,
      COALESCE(bamboohr_employment_status.valid_from_date, cleaned.effective_end_date) as effective_end_date,
      cleaned.department,
      cleaned.division,
      cleaned.entity,
      cleaned.reports_to,
      sheetload_job_roles.job_role --- This will only appear for records prior to 2020-02-28 -- after this data populates in bamboohr_job_role
    FROM cleaned
    LEFT JOIN sheetload_job_roles
      ON sheetload_job_roles.job_title = cleaned.job_title
    LEFT JOIN bamboohr_employment_status
      ON bamboohr_employment_status.employee_id = cleaned.employee_id 
      AND bamboohr_employment_status.valid_from_date BETWEEN cleaned.effective_date AND COALESCE(cleaned.effective_end_date, {{max_date_in_bamboo_analyses()}})

)

SELECT *
FROM joined