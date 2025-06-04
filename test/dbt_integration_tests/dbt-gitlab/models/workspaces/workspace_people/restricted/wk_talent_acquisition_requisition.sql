WITH greenhouse_job_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_job_custom_fields_source') }}
),

greenhouse_hiring_team_source AS (
  SELECT *
  FROM {{ ref('greenhouse_hiring_team_source') }}
),

greenhouse_organizations_source AS (
  SELECT *
  FROM {{ ref('greenhouse_organizations_source') }}
),

greenhouse_jobs_departments_source AS (
  SELECT *
  FROM {{ ref('greenhouse_jobs_departments_source') }}
),

greenhouse_departments_source AS (
  SELECT *
  FROM {{ ref('greenhouse_departments_source') }}
),

rpt_greenhouse_current_openings AS (
  SELECT *
  FROM {{ ref('rpt_greenhouse_current_openings') }}
),

job AS (
  SELECT *
  FROM
    {{ ref('greenhouse_jobs_source') }}
),
users_raw AS (
  SELECT *
  FROM {{ source('greenhouse', 'users') }}
),

job_custom AS (

  SELECT
    job_id AS job_custom_id,
    MAX(CASE job_custom_field
      WHEN 'Bonus'
        THEN job_custom_field_display_value
    END)   AS bonus,

    MAX(CASE job_custom_field
      WHEN 'Hiring Plan'
        THEN job_custom_field_display_value
    END)   AS hiring_plan,

    MAX(CASE job_custom_field
      WHEN 'Job Description'
        THEN job_custom_field_display_value
    END)   AS job_description,

    MAX(CASE job_custom_field
      WHEN 'Colorado Salary'
        THEN job_custom_field_display_value
    END)   AS colorado_salary,

    MAX(CASE job_custom_field
      WHEN 'Employment Type'
        THEN job_custom_field_display_value
    END)   AS employment_type,

    MAX(CASE job_custom_field
      WHEN 'Job Family URL'
        THEN job_custom_field_display_value
    END)   AS job_family_url,

    MAX(CASE job_custom_field
      WHEN 'Job Grade'
        THEN job_custom_field_display_value
    END)   AS job_grade,

    MAX(CASE job_custom_field
      WHEN 'Quota Coverage Type'
        THEN job_custom_field_display_value
    END)   AS quota_coverage_type,

    MAX(CASE job_custom_field
      WHEN 'Hourly'
        THEN job_custom_field_display_value
    END)   AS hourly,

    MAX(CASE job_custom_field
      WHEN 'Finance ID'
        THEN job_custom_field_display_value
    END)   AS finance_id,

    MAX(CASE job_custom_field
      WHEN 'Planned Hire Date'
        THEN job_custom_field_display_value
    END)   AS planned_hire_date,

    MAX(CASE job_custom_field
      WHEN 'Confidential?'
        THEN job_custom_field_display_value
    END)   AS confidential,

    MAX(CASE job_custom_field
      WHEN 'NY/NJ Salary'
        THEN job_custom_field_display_value
    END)   AS ny_nj_salary,

    MAX(CASE job_custom_field
      WHEN 'Hiring Manager'
        THEN job_custom_field_display_value
    END)   AS hiring_manager,

    MAX(CASE job_custom_field
      WHEN 'Quota or Non-Quota Carrying'
        THEN job_custom_field_display_value
    END)   AS quota_carrying,

    MAX(CASE job_custom_field
      WHEN 'Options'
        THEN job_custom_field_display_value
    END)   AS options,

    MAX(CASE job_custom_field
      WHEN 'Type'
        THEN job_custom_field_display_value
    END)   AS type,

    MAX(CASE job_custom_field
      WHEN 'Salary'
        THEN job_custom_field_display_value
    END)   AS salary

  FROM greenhouse_job_custom_fields_source
  GROUP BY 1


),

job_team_stage AS (
  SELECT
    tm.*,
    usr.employee_id,
    usr.full_name,
    ROW_NUMBER() OVER (PARTITION BY tm.job_id, tm.hiring_team_role ORDER BY tm.user_id DESC) AS tm_rank
  FROM greenhouse_hiring_team_source AS tm
  LEFT JOIN users_raw AS usr ON tm.user_id = usr.id
  QUALIFY tm_rank = 1
),

job_team_final AS (
  SELECT
    job_id                                                                 AS team_job_id,
    MAX(IFF(tm.hiring_team_role = 'Hiring Manager', tm.employee_id, NULL)) AS hiring_manager_id,
    MAX(IFF(tm.hiring_team_role = 'Recruiter', tm.employee_id, NULL))      AS recruiter_id,
    MAX(IFF(tm.hiring_team_role = 'Coordinator', tm.employee_id, NULL))    AS coordinator_id,
    MAX(IFF(tm.hiring_team_role = 'Sourcer', tm.employee_id, NULL))        AS sourcer_id,
    MAX(IFF(tm.hiring_team_role = 'Hiring Manager', tm.full_name, NULL))   AS hiring_manager_name,
    MAX(IFF(tm.hiring_team_role = 'Recruiter', tm.full_name, NULL))        AS recruiter_name,
    MAX(IFF(tm.hiring_team_role = 'Coordinator', tm.full_name, NULL))      AS coordinator_name,
    MAX(IFF(tm.hiring_team_role = 'Sourcer', tm.full_name, NULL))          AS sourcer_name
  FROM job_team_stage AS tm
  GROUP BY 1
),

organization AS (
  SELECT
    organization_id,
    organization_name
  FROM greenhouse_organizations_source
),

department AS (

  WITH source AS (
    SELECT *
    FROM greenhouse_departments_source
  ),

  greenhouse_departments (department_name, department_id, hierarchy_id, hierarchy_name) AS (
    SELECT
      department_name,
      department_id,
      TO_ARRAY(department_id)   AS hierarchy_id,
      TO_ARRAY(department_name) AS hierarchy_name
    FROM source
    WHERE parent_id IS NULL
    UNION ALL
    SELECT
      iteration.department_name,
      iteration.department_id,
      ARRAY_APPEND(anchor.hierarchy_id, iteration.department_id)     AS hierarchy_id,
      ARRAY_APPEND(anchor.hierarchy_name, iteration.department_name) AS hierarchy_name
    FROM source AS iteration
    INNER JOIN greenhouse_departments AS anchor
      ON iteration.parent_id = anchor.department_id
  ),

  departments AS (
    SELECT
      department_name,
      department_id,
      ARRAY_SIZE(hierarchy_id)   AS hierarchy_level,
      hierarchy_id,
      hierarchy_name,
      hierarchy_name[0]::VARCHAR AS level_1,
      hierarchy_name[1]::VARCHAR AS level_2,
      hierarchy_name[2]::VARCHAR AS level_3
    FROM greenhouse_departments
  ),

  job_departments AS (


    SELECT *
    FROM greenhouse_jobs_departments_source
    -- Table is many to many (job_id to department_id) with the lowest level created first
    QUALIFY ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_department_created_at ASC) = 1

  )

  SELECT
    job_departments.job_id,
    job_departments.department_id,
    level_1                                                                       AS cost_center_name,
    IFF(level_2 ILIKE 'inactive - %', RIGHT(level_2, LEN(level_2) - 11), level_2) AS department_name,
    level_3                                                                       AS sub_department_name
  FROM job_departments
  LEFT JOIN departments
    ON job_departments.department_id = departments.department_id


),

openings AS (

  SELECT
    job_id,
    COUNT(DISTINCT job_opening_id)
  FROM rpt_greenhouse_current_openings
  GROUP BY 1


),



stage AS (
  SELECT
    job.*,
    job_custom.*,
    job_team_final.*,
    department.cost_center_name,
    department.department_name,
    department.sub_department_name
  FROM job
  LEFT JOIN job_custom ON job.job_id = job_custom.job_custom_id
  LEFT JOIN job_team_final ON job.job_id = job_team_final.team_job_id
  LEFT JOIN department ON job.department_id = department.department_id
    AND job.job_id = department.job_id

)

SELECT
  job_id,
  requisition_id,
  'GitLab' AS organization_name,
  department_name,
  sub_department_name,
  cost_center_name,
  job_name,
  job_status,
  is_template,
  job_created_at,
  job_opened_at,
  job_closed_at,
  job_updated_at,
  hiring_plan,
  employment_type,
  job_grade,
  quota_coverage_type,
  hourly,
  confidential,
  finance_id,
  planned_hire_date,
  quota_carrying,
  type,
  hiring_manager_id,
  recruiter_id,
  coordinator_id,
  sourcer_id,
  hiring_manager_name,
  recruiter_name,
  sourcer_name,
  coordinator_name
FROM stage
