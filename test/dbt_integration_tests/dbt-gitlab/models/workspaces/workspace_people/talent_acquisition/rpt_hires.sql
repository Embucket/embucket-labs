{{ simple_cte([
    ('bamboohr_mapping','bamboohr_id_employee_number_mapping'),
    ('applications','greenhouse_applications_source'),
    ('sources','greenhouse_sources_source'),
    ('offers','greenhouse_offers_source'),
    ('openings','greenhouse_openings_source'),
    ('departments','wk_prep_greenhouse_departments'),
    ('application_jobs','greenhouse_applications_jobs_source'),
    ('jobs','greenhouse_jobs_source')
])}}

, job_departments AS (

    SELECT *
    FROM {{ ref('greenhouse_jobs_departments_source') }}
    -- Table is many to many (job_id to department_id) with the lowest level created first
    QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY job_department_created_at ASC) = 1
    
)

SELECT
  applications.candidate_id,
  applications.application_id,
  bamboohr_mapping.region,
  jobs.job_id,
  jobs.job_name,
  applications.application_recruiter,
  sources.source_name,
  sources.source_type,
  applications.applied_at,
  openings.job_opened_at            AS job_opening_opened_at,
  openings.job_closed_at            AS job_opening_closed_at,
  openings.target_start_date        AS job_opening_target_start_date,
  offers.sent_at                    AS offer_sent_at,
  offers.resolved_at                AS offer_accepted_at,
  departments.department_name       AS greenhouse_department_name,
  departments.level_1               AS greenhouse_department_level_1,
  departments.level_2               AS greenhouse_department_level_2,
  departments.level_3               AS greenhouse_department_level_3
FROM offers
LEFT JOIN applications
  ON offers.application_id = applications.application_id
LEFT JOIN sources
  ON applications.source_id = sources.source_id
LEFT JOIN openings
  ON offers.application_id = openings.hired_application_id
LEFT JOIN bamboohr_mapping
  ON applications.candidate_id = bamboohr_mapping.greenhouse_candidate_id
LEFT JOIN application_jobs
  ON offers.application_id = application_jobs.application_id
LEFT JOIN job_departments
  ON application_jobs.job_id = job_departments.job_id
LEFT JOIN departments
  ON job_departments.department_id = departments.department_id
LEFT JOIN jobs
  ON application_jobs.job_id = jobs.job_id
WHERE offers.offer_status = 'accepted'

