WITH greenhouse_openings AS (

    SELECT * 
    FROM {{ref('greenhouse_openings_source')}}

), greenhouse_jobs AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs_source')}}

), greenhouse_department AS (

    SELECT 
      department_id,
      department_name,
      CASE WHEN LOWER(department_name) LIKE '%enterprise sales%'
           THEN 'Enterprise Sales'
           WHEN LOWER(department_name) LIKE '%commercial sales%' 
           THEN 'Commercial Sales'
           ELSE TRIM(department_name) END AS department_modified
    FROM {{ref('greenhouse_departments_source')}}
      
), greenhouse_organization AS (

    SELECT * 
    FROM {{ref('greenhouse_organizations_source')}}


), greenhouse_opening_custom_fields AS (

    SELECT 
      opening_id, 
      {{ dbt_utils.pivot(
          'opening_custom_field', 
          dbt_utils.get_column_values(ref('greenhouse_opening_custom_fields_source'), 'opening_custom_field'),
          agg = 'MAX',
          then_value = 'opening_custom_field_display_value',
          else_value = 'NULL',
          quote_identifiers = False
      ) }}
    FROM {{ref('greenhouse_opening_custom_fields_source')}}
    GROUP BY opening_id

), greenhouse_recruiting_xf AS (

    SELECT * 
    FROM {{ref('greenhouse_recruiting_xf')}}

), division_mapping AS (
  
    SELECT DISTINCT 
      date_actual,   
      division_mapped_current as division, 
      department AS department
    FROM {{ref('employee_directory_intermediate')}}

), cost_center_mapping AS (

    SELECT *
    FROM {{ref('cost_center_division_department_mapping_current')}}
 
), hires AS (
  
    SELECT *
    FROM {{ref('employee_directory')}}


), greenhouse_jobs_offices AS (

    SELECT * 
    FROM {{ref('greenhouse_jobs_offices_source')}}

), greenhouse_offices_sources AS (

    SELECT * 
    FROM {{ref('greenhouse_offices_source')}}
    WHERE office_name IS NOT NULL
  
), office AS (
  
    SELECT 
      greenhouse_jobs_offices.job_id, 
      listagg(DISTINCT office_name,', ') as office_name
    FROM greenhouse_jobs_offices
    LEFT JOIN greenhouse_offices_sources
      ON greenhouse_offices_sources.office_id = greenhouse_jobs_offices.office_id
    WHERE office_name IS NOT NULL
    GROUP BY 1


), aggregated AS (

    SELECT 
      greenhouse_openings.job_opening_id,
      greenhouse_openings.job_id,
      greenhouse_opening_custom_fields.finance_id               AS ghp_id,
      greenhouse_jobs.job_created_at, 
      greenhouse_jobs.job_status,
      greenhouse_openings.opening_id, 
      greenhouse_recruiting_xf.is_hired_in_bamboo,
      greenhouse_recruiting_xf.candidate_target_hire_date,
      greenhouse_recruiting_xf.offer_status,
      greenhouse_openings.target_start_date,
      greenhouse_openings.job_opened_at                         AS opening_date,
      greenhouse_openings.job_closed_at                         AS closing_date,
      greenhouse_openings.close_reason,
      greenhouse_jobs.job_name                                  AS job_title, 
      greenhouse_department.department_name,
      COALESCE(division_mapping.division, 
               cost_center_mapping.division)                    AS division,
      greenhouse_opening_custom_fields.hiring_manager,
      greenhouse_opening_custom_fields.type                     AS opening_type,
      hires.employee_id,
      office.office_name                                        AS region
    FROM greenhouse_openings
    LEFT JOIN greenhouse_jobs
      ON greenhouse_openings.job_id = greenhouse_jobs.job_id 
    LEFT JOIN greenhouse_department 
      ON greenhouse_department.department_id = greenhouse_jobs.department_id
    LEFT JOIN greenhouse_opening_custom_fields 
      ON greenhouse_opening_custom_fields.opening_id = greenhouse_openings.job_opening_id  
    LEFT JOIN greenhouse_recruiting_xf
      ON greenhouse_openings.hired_application_id = greenhouse_recruiting_xf.application_id 
    LEFT JOIN division_mapping
      ON division_mapping.department = greenhouse_department.department_modified
      AND division_mapping.date_actual = DATE_TRUNC(DAY,greenhouse_openings.job_opened_at)
    LEFT JOIN cost_center_mapping
      ON cost_center_mapping.department = greenhouse_department.department_modified
    LEFT JOIN hires
      ON hires.greenhouse_candidate_id = greenhouse_recruiting_xf.candidate_id
    LEFT JOIN office
      ON office.job_id = greenhouse_openings.job_id  
    WHERE greenhouse_jobs.job_opened_at IS NOT NULL 
)

SELECT *
FROM aggregated
