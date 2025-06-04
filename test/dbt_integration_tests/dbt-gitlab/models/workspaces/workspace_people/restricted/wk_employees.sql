WITH bamboo_mapping AS (

	SELECT *
	FROM {{ ref('bamboohr_id_employee_number_mapping' )}}

),

gender_workday AS (

  SELECT 
    employee_id,
    gender,
    gender_dropdown
  FROM {{ ref('workday_employee_mapping_source')}}
  WHERE is_current

),

email AS (

  SELECT 
    employee_id,
	  last_work_email
  FROM {{ref('employee_directory')}}
  GROUP BY 1, 2 
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY last_work_email DESC) = 1

),

dob AS (

  SELECT 
    employee_id,
    gitlab_username,
    date_of_birth
  FROM {{ref('workday_employee_mapping_source')}}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY uploaded_at DESC) = 1

)  

SELECT 
	bamboo_mapping.employee_number,
	bamboo_mapping.employee_id,
	bamboo_mapping.first_name,
	bamboo_mapping.last_name,
	bamboo_mapping.hire_date,
	bamboo_mapping.termination_date,
	bamboo_mapping.first_inactive_date,
	bamboo_mapping.age_cohort,
	bamboo_mapping.gender                                                       AS gender,
	COALESCE(gender_workday.gender_dropdown, bamboo_mapping.gender)             AS gender_identity,
	bamboo_mapping.ethnicity,
	bamboo_mapping.country,
	bamboo_mapping.nationality,
	bamboo_mapping.region,
	bamboo_mapping.region_modified,
	bamboo_mapping.gender_region,
	bamboo_mapping.greenhouse_candidate_id,
	bamboo_mapping.last_updated_date,
	bamboo_mapping.urg_group,
	email.last_work_email,
	dob.gitlab_username,
	dob.date_of_birth
FROM bamboo_mapping
LEFT JOIN email 
  ON bamboo_mapping.employee_id = email.employee_id
LEFT JOIN dob 
  ON bamboo_mapping.employee_id = dob.employee_id
LEFT JOIN gender_workday 
  ON bamboo_mapping.employee_id = gender_workday.employee_id
