WITH mapping as (

    SELECT *
    FROM {{ref('bamboohr_id_employee_number_mapping')}}

), bamboohr_directory AS (

    SELECT *
    FROM {{ ref ('blended_directory_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY uploaded_at DESC) =1 

), department_info as (

    SELECT 
      employee_id,
      LAST_VALUE(job_title) RESPECT NULLS 
          OVER (PARTITION BY employee_id ORDER BY effective_date , job_sequence) AS last_job_title,
      LAST_VALUE(reports_to) RESPECT NULLS
          OVER (PARTITION BY employee_id ORDER BY effective_date, job_sequence) AS last_supervisor,
      LAST_VALUE(department) RESPECT NULLS
          OVER (PARTITION BY employee_id ORDER BY effective_date, job_sequence) AS last_department,
      LAST_VALUE(division) RESPECT NULLS
          OVER  (PARTITION BY employee_id ORDER BY effective_date, job_sequence) AS last_division       
    FROM {{ ref ('blended_job_info_source') }}

), cost_center AS (

    SELECT
      employee_id,
      LAST_VALUE(cost_center) RESPECT NULLS
          OVER ( PARTITION BY employee_id ORDER BY effective_date) AS last_cost_center
    FROM {{ ref ('bamboohr_job_role') }}

), location_factor as (

    SELECT 
      DISTINCT bamboo_employee_number,
      FIRST_VALUE(location_factor) OVER ( PARTITION BY bamboo_employee_number ORDER BY valid_from) AS hire_location_factor
    FROM {{ ref('employee_location_factor_snapshots') }}

), initial_hire AS (
    
    SELECT 
      employee_id,
      effective_date as hire_date
    FROM {{ref('blended_employment_status_source')}}
    WHERE employment_status != 'Terminated'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY effective_date) = 1

), rehire AS (

    SELECT 
      employee_id,
      is_rehire,
      valid_from_date as rehire_date
    FROM {{ref('bamboohr_employment_status_xf')}}
    WHERE is_rehire='True'

), final AS (

    SELECT 
      DISTINCT mapping.employee_id,
      mapping.employee_number,
      mapping.first_name,
      mapping.last_name,
      mapping.first_name || ' ' || mapping.last_name                            AS full_name,
      mapping.region_modified                                                   AS region_modified,
      mapping.country                                                           AS country,
      bamboohr_directory.work_email                                             AS last_work_email,
      IFF(rehire.is_rehire = 'True', initial_hire.hire_date, mapping.hire_date) AS hire_date,
      rehire.rehire_date,
      mapping.termination_date,
      mapping.first_inactive_date,
      department_info.last_job_title,
      department_info.last_supervisor,
      department_info.last_department,
      department_info.last_division,
      cost_center.last_cost_center,
      location_factor.hire_location_factor,
      mapping.greenhouse_candidate_id
    FROM mapping
    LEFT JOIN bamboohr_directory
      ON bamboohr_directory.employee_id = mapping.employee_id
    LEFT JOIN department_info
      ON mapping.employee_id = department_info.employee_id
    LEFT JOIN location_factor
      ON location_factor.bamboo_employee_number = mapping.employee_number
    LEFT JOIN initial_hire 
      ON initial_hire.employee_id = mapping.employee_id
    LEFT JOIN rehire
      ON rehire.employee_id = mapping.employee_id
    LEFT JOIN cost_center
      ON cost_center.employee_id = mapping.employee_id  

)

SELECT * 
FROM final
