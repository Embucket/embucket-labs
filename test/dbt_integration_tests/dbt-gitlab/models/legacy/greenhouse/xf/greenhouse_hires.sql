WITH applications AS (

    SELECT *,
      ROW_NUMBER() OVER (PARTITION BY candidate_id ORDER BY applied_at)     AS greenhouse_candidate_row_number
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE application_status = 'hired'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}
    WHERE offer_status = 'accepted'

), openings AS (
    
    SELECT *
    FROM {{ ref ('greenhouse_openings_source') }}

), greenhouse_opening_custom_fields AS (

    SELECT *
    FROM  {{ ref ('greenhouse_opening_custom_fields') }}

), bamboo_hires AS (
 
    SELECT *
    FROM  {{ ref ('employee_directory') }}

), bamboohr_mapping AS (

    SELECT *
    FROM  {{ ref ('bamboohr_id_employee_number_mapping') }}
   
), division_department AS (

    SELECT *
    FROM  {{ ref ('employee_directory_intermediate') }}
    
), joined AS (

    SELECT 
      openings.job_id,
      applications.application_id,  
      applications.candidate_id, 
      bamboo_hires.employee_id,
      bamboo_hires.full_name                                        AS employee_name,
      bamboohr_mapping.region,
      offers.start_date                                             AS candidate_target_hire_date, 
      applications.applied_at, 
      applications.greenhouse_candidate_row_number,
      IFF(applications.greenhouse_candidate_row_number = 1 
            AND applied_at < bamboo_hires.hire_date, 
              bamboo_hires.hire_date, candidate_target_hire_date)   AS hire_date_mod,
      is_hire_date,
      is_rehire_date,
      CASE WHEN greenhouse_candidate_row_number = 1 
            THEN 'hire'
           WHEN offers.start_date = bamboo_hires.rehire_date
            THEN 'rehire'
           WHEN greenhouse_candidate_row_number>1 
            THEN 'transfer'
           ELSE NULL END                                            AS hire_type,
      greenhouse_opening_custom_fields.job_opening_type,
      division_department.division_mapped_current                   AS division,
      division_department.department_modified                       AS department,
      division_department.employment_status,
      division_department.is_promotion
    FROM applications
    LEFT JOIN offers
      ON offers.application_id = applications.application_id
    LEFT JOIN bamboo_hires 
      ON bamboo_hires.greenhouse_candidate_id = applications.candidate_id
    LEFT JOIN bamboohr_mapping
      ON bamboo_hires.employee_id = bamboohr_mapping.employee_id
    LEFT JOIN openings
      ON openings.hired_application_id = applications.application_id
    LEFT JOIN greenhouse_opening_custom_fields
      ON greenhouse_opening_custom_fields.job_opening_id = openings.job_opening_id
    LEFT JOIN division_department
      ON division_department.employee_id = bamboo_hires.employee_id
      AND division_department.date_actual =  IFF(applications.greenhouse_candidate_row_number = 1 
            AND applied_at < bamboo_hires.hire_date, 
              bamboo_hires.hire_date, offers.start_date)

), final AS (    

    SELECT
      {{ dbt_utils.generate_surrogate_key(['application_id', 'candidate_id',]) }}  AS unique_key,
      job_id,
      application_id,
      candidate_id,
      employee_id,
      employee_name,
      region,
      greenhouse_candidate_row_number,
      hire_date_mod,
      hire_type,
      job_opening_type,
      IFF(employment_status IS NOT NULL,TRUE,FALSE)                             AS hired_in_bamboohr,
      division,
      department
    FROM joined 
    WHERE is_promotion != TRUE --removing promotions

)

SELECT *
FROM final
