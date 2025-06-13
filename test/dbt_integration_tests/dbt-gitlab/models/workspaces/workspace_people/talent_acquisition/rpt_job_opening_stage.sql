{{ simple_cte([
    ('job_openings','greenhouse_openings_source'),
    ('jobs','greenhouse_jobs_source'),
    ('application_jobs','greenhouse_applications_jobs_source'),
    ('applications','greenhouse_applications_source'),
    ('sources','greenhouse_sources_source'),
    ('departments','wk_prep_greenhouse_departments')
])}}

, job_departments AS (

    SELECT *
    FROM {{ ref('greenhouse_jobs_departments_source') }}
    -- Table is many to many (job_id to department_id) with the lowest level created first
    QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY job_department_created_at ASC) = 1

), application_stages AS (

    SELECT *
    FROM {{ ref('greenhouse_application_stages_source') }}
    -- Table can contain duplicate records
    QUALIFY ROW_NUMBER() OVER (PARTITION BY application_id,modified_stage_id,stage_entered_on ORDER BY stage_entered_on) = 1

)


  SELECT
    job_openings.job_id,
    job_openings.job_opening_id,
    application_stages.stage_entered_on          AS stage_entered_date,
    application_stages.stage_exited_on           AS stage_exited_date,
    application_stages.application_stage_name,
    application_stages.modified_stage_id,
    applications.application_id,
    sources.source_name,
    sources.source_type,
    departments.department_name                  AS greenhouse_department_name,
    departments.level_1                          AS greenhouse_department_level_1,
    departments.level_2                          AS greenhouse_department_level_2,
    departments.level_3                          AS greenhouse_department_level_3
  FROM job_openings
  LEFT JOIN jobs
    ON job_openings.job_id = jobs.job_id
  LEFT JOIN application_jobs
    ON job_openings.job_id = application_jobs.job_id
  LEFT JOIN applications
    ON application_jobs.application_id = applications.application_id
  LEFT JOIN application_stages
    ON application_jobs.application_id = application_stages.application_id
  LEFT JOIN sources
    ON applications.source_id = sources.source_id
  LEFT JOIN job_departments
    ON job_openings.job_id = job_departments.job_id
  LEFT JOIN departments
    ON job_departments.department_id = departments.department_id
  WHERE application_stages.stage_entered_on IS NOT NULL
    AND jobs.job_opened_at IS NOT NULL
    AND applications.application_id IS NOT NULL
    AND application_stages.modified_stage_id IS NOT NULL
