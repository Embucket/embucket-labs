{{ config({
    "materialized":"table",
    })
}}

WITH RECURSIVE employee_directory AS (

  SELECT
    employee_id,
    employee_number,
    first_name,
    last_name,
    hire_date,
    rehire_date,
    region_modified,
    country,
    termination_date,
    first_inactive_date,
    hire_location_factor,
    last_work_email,
    (COALESCE(first_name,'') || ' ' || COALESCE(last_name,'')) AS full_name
  FROM {{ ref('employee_directory') }}

),

date_details AS (

  SELECT *
  FROM {{ ref('date_details') }}

),

department_info AS (

  SELECT *
  FROM {{ ref('bamboohr_job_info_current_division_base') }}

),

job_role AS (

  SELECT *
  FROM {{ ref('bamboohr_job_role') }}

),

location_factor AS (

  SELECT *
  FROM {{ ref('employee_location_factor_snapshots') }}

),

employment_status AS (

  SELECT *
  FROM {{ ref('bamboohr_employment_status_xf') }}

),

promotion AS (

  SELECT
    employee_id,
    effective_date,
    compensation_change_reason
  FROM {{ ref('blended_compensation_source') }}
  WHERE compensation_change_reason = 'Promotion'
  GROUP BY 1, 2, 3

),

direct_reports AS (

  SELECT
    date_actual,
    reports_to,
    COUNT(employee_id) AS total_direct_reports
  FROM (
      SELECT
        date_details.date_actual,
        employee_directory.employee_id,
        department_info.reports_to
      FROM date_details
      LEFT JOIN employee_directory
        ON employee_directory.hire_date::DATE <= date_details.date_actual
          AND COALESCE(
            employee_directory.first_inactive_date::DATE, {{ max_date_in_bamboo_analyses() }}
          ) >= date_details.date_actual
      LEFT JOIN department_info
        ON employee_directory.employee_id = department_info.employee_id
          AND date_details.date_actual BETWEEN department_info.effective_date
          AND COALESCE(department_info.effective_end_date, {{ max_date_in_bamboo_analyses() }})
    )
  GROUP BY 1, 2
  HAVING total_direct_reports > 0

),

job_info_mapping_historical AS (

  SELECT
    department_info.employee_id,
    department_info.job_title,
    IFF(
      department_info.job_title = 'Manager, Field Marketing',
      'Leader',
      COALESCE(job_role.job_role, department_info.job_role)
    ) AS job_role,
    CASE WHEN department_info.job_title = 'Group Manager, Product'
      THEN '9.5'
      WHEN department_info.job_title = 'Manager, Field Marketing'
        THEN '8'
      ELSE job_role.job_grade END AS job_grade,
    ROW_NUMBER() OVER (
      PARTITION BY department_info.employee_id ORDER BY date_details.date_actual
    ) AS job_grade_event_rank
  FROM date_details
  LEFT JOIN department_info
    ON
      date_details.date_actual BETWEEN department_info.effective_date AND COALESCE(
        department_info.effective_end_date, {{ max_date_in_bamboo_analyses() }}
      )
  LEFT JOIN job_role
    ON job_role.employee_id = department_info.employee_id
      AND date_details.date_actual BETWEEN job_role.effective_date AND COALESCE(
        job_role.next_effective_date, {{ max_date_in_bamboo_analyses() }}
      )
  WHERE job_role.job_grade IS NOT NULL
    -- Using the 1st time we captured job_role and grade
    -- to identify classification for historical records

),

employment_status_records_check AS (

  SELECT
    employee_id,
    MIN(valid_from_date) AS employment_status_first_value
  FROM {{ ref('bamboohr_employment_status_xf') }}
  GROUP BY 1

),

cost_center_prior_to_bamboo AS (

  SELECT *
  FROM {{ ref('cost_center_division_department_mapping') }}

),

sheetload_engineering_speciality AS (

  SELECT *
  FROM {{ ref('sheetload_engineering_speciality_prior_to_capture') }}

),

bamboohr_discretionary_bonuses_xf AS (

  SELECT *
  FROM {{ ref('bamboohr_directionary_bonuses_xf') }}

),

fct_work_email AS (

  SELECT *
  FROM {{ ref('bamboohr_work_email') }}

),

enriched AS (

  SELECT
    employee_directory.*,
    date_details.date_actual,
    department_info.job_title,
    department_info.department,
    department_info.department_modified,
    department_info.department_grouping,
    department_info.division,
    department_info.division_mapped_current,
    department_info.division_grouping,
    department_info.reports_to,
    department_info.reports_to_id,
    location_factor.location_factor AS location_factor,
    location_factor.locality,
    job_role.gitlab_username,
    job_role.region,
    job_role.sales_geo_differential,
    -- to capture speciality for engineering prior to 2020.09.30 
    -- we are using sheetload, and capturing from bamboohr afterwards
    direct_reports.total_direct_reports,
    bamboohr_discretionary_bonuses_xf.total_discretionary_bonuses AS discretionary_bonus,
    COALESCE(fct_work_email.work_email, employee_directory.last_work_email) AS work_email,
    COALESCE(job_role.cost_center,
      cost_center_prior_to_bamboo.cost_center) AS cost_center,
    IFF(date_details.date_actual BETWEEN '2019-11-01' AND '2020-02-27'
      AND job_info_mapping_historical.job_role IS NOT NULL,
      job_info_mapping_historical.job_role,
      COALESCE(job_role.job_role, department_info.job_role)) AS job_role,
    IFF(date_details.date_actual BETWEEN '2019-11-01' AND '2020-02-27',
      job_info_mapping_historical.job_grade,
      job_role.job_grade) AS job_grade,
    COALESCE(
      sheetload_engineering_speciality.speciality, job_role.jobtitle_speciality
    ) AS jobtitle_speciality,
    IFF(employee_directory.hire_date = date_details.date_actual
      OR employee_directory.rehire_date = date_details.date_actual, TRUE, FALSE) AS is_hire_date,
    IFF(
      employment_status.employment_status = 'Terminated', TRUE, FALSE
    ) AS is_termination_date,
    IFF(employee_directory.rehire_date = date_details.date_actual, TRUE, FALSE) AS is_rehire_date,
    -- for the diversity KPIs we are looking to understand senior leadership
    -- representation and do so by job grade instead of role        
    IFF(
      employee_directory.hire_date < employment_status_records_check.employment_status_first_value,
      'Active', employment_status.employment_status) AS employment_status,
    CASE IFF(
           date_details.date_actual BETWEEN '2019-11-01' AND '2020-02-27'
           AND job_info_mapping_historical.job_role IS NOT NULL,
           job_info_mapping_historical.job_role,
           COALESCE(job_role.job_role, department_info.job_role))
      WHEN 'Intern'
        THEN 'Individual Contributor'
      WHEN 'Individual Contributor'
        THEN 'Individual Contributor'
      WHEN 'Manager'
        THEN 'Manager'
      WHEN 'Director'
        THEN 'Director+'
      WHEN 'Vice President'
        THEN 'Director+'
      WHEN 'Leader'
        THEN 'Director+'
      WHEN 'Chief Executive Officer'
        THEN 'Director+'
      ELSE IFF(date_details.date_actual BETWEEN '2019-11-01' AND '2020-02-27'
             AND job_info_mapping_historical.job_role IS NOT NULL,
             job_info_mapping_historical.job_role,
             COALESCE(job_role.job_role, department_info.job_role)) END AS job_role_modified,
    IFF(promotion.compensation_change_reason IS NOT NULL, TRUE, FALSE) AS is_promotion,
    ROW_NUMBER() OVER
    (PARTITION BY employee_directory.employee_id ORDER BY date_details.date_actual) AS tenure_days
  FROM date_details
  LEFT JOIN employee_directory
    ON employee_directory.hire_date::DATE <= date_details.date_actual
      AND COALESCE(
        employee_directory.termination_date::DATE, {{ max_date_in_bamboo_analyses() }}
      ) >= date_details.date_actual
      -- active employees that have been rehired will have a termination date less than 
      -- the rehire date and they need to be included while excluding those terminated after
      -- the rehire date
      OR (employee_directory.rehire_date::DATE <= date_details.date_actual
      AND IFF(employee_directory.first_inactive_date > employee_directory.rehire_date, employee_directory.first_inactive_date,
            {{ max_date_in_bamboo_analyses() }}) >= date_details.date_actual)
  LEFT JOIN department_info
    ON employee_directory.employee_id = department_info.employee_id
      AND date_details.date_actual BETWEEN department_info.effective_date
      AND COALESCE(department_info.effective_end_date::DATE, {{ max_date_in_bamboo_analyses() }})
  LEFT JOIN direct_reports
    ON direct_reports.date_actual = date_details.date_actual
      AND direct_reports.reports_to = employee_directory.full_name
  LEFT JOIN location_factor
    ON employee_directory.employee_number::VARCHAR = location_factor.bamboo_employee_number::VARCHAR
      AND location_factor.valid_from <= date_details.date_actual
      AND COALESCE(
        location_factor.valid_to::DATE, {{ max_date_in_bamboo_analyses() }}
      ) >= date_details.date_actual
  LEFT JOIN employment_status
    ON employee_directory.employee_id = employment_status.employee_id
      AND (
        date_details.date_actual = employment_status.valid_from_date
        AND employment_status.employment_status = 'Terminated'
        OR date_details.date_actual
        BETWEEN employment_status.valid_from_date AND employment_status.valid_to_date
      )
  LEFT JOIN employment_status_records_check
    ON employee_directory.employee_id = employment_status_records_check.employee_id
  LEFT JOIN cost_center_prior_to_bamboo
    ON department_info.department = cost_center_prior_to_bamboo.department
      AND department_info.division = cost_center_prior_to_bamboo.division
      AND date_details.date_actual BETWEEN cost_center_prior_to_bamboo.effective_start_date
      AND COALESCE(cost_center_prior_to_bamboo.effective_end_date, '2020-05-07')
  ---Starting 2020.05.08 we start capturing cost_center in bamboohr
  LEFT JOIN job_role
    ON employee_directory.employee_id = job_role.employee_id
      AND date_details.date_actual BETWEEN job_role.effective_date AND COALESCE(
        job_role.next_effective_date, {{ max_date_in_bamboo_analyses() }}
      )
  LEFT JOIN job_info_mapping_historical
    ON employee_directory.employee_id = job_info_mapping_historical.employee_id
      AND job_info_mapping_historical.job_title = department_info.job_title
      AND job_info_mapping_historical.job_grade_event_rank = 1
  ---tying data based on 2020-02-27 date to historical data --
  LEFT JOIN promotion
    ON promotion.employee_id = employee_directory.employee_id
      AND date_details.date_actual = promotion.effective_date
  LEFT JOIN sheetload_engineering_speciality
    ON employee_directory.employee_id = sheetload_engineering_speciality.employee_id
      AND date_details.date_actual BETWEEN sheetload_engineering_speciality.speciality_start_date
      AND COALESCE(sheetload_engineering_speciality.speciality_end_date, '2020-09-30')
  ---Post 2020.09.30 we will capture engineering speciality from bamboohr
  LEFT JOIN bamboohr_discretionary_bonuses_xf
    ON employee_directory.employee_id = bamboohr_discretionary_bonuses_xf.employee_id
      AND date_details.date_actual = bamboohr_discretionary_bonuses_xf.bonus_date
  LEFT JOIN fct_work_email
    ON employee_directory.employee_id = fct_work_email.employee_id
      AND date_details.date_actual
      BETWEEN fct_work_email.valid_from_date AND fct_work_email.valid_to_date
  WHERE employee_directory.employee_id IS NOT NULL

),

base_layers AS (

  SELECT
    date_actual,
    reports_to,
    full_name,
    ARRAY_CONSTRUCT(reports_to, full_name) AS lineage
  FROM enriched
  WHERE NULLIF(reports_to, '') IS NOT NULL
  AND full_name != reports_to -- Sid is reported as reporting to himself

),

layers (date_actual, employee, manager, lineage, layers_count
)

AS (

  SELECT
    date_actual,
    full_name AS employee,
    reports_to AS manager,
    lineage AS lineage,
    1 AS layers_count
  FROM base_layers
  WHERE manager IS NOT NULL

  UNION ALL

  SELECT
    layers.date_actual,
    base_layers.full_name AS employee,
    base_layers.reports_to AS manager,
    ARRAY_PREPEND(layers.lineage, base_layers.reports_to) AS lineage,
    (layers.layers_count + 1) AS layers_count
  FROM layers
  INNER JOIN base_layers
    ON layers.date_actual = base_layers.date_actual
      AND base_layers.reports_to = layers.employee

),

calculated_layers AS (

  SELECT
    date_actual,
    employee,
    MAX(layers_count) AS layers
  FROM layers
  GROUP BY 1, 2

)

SELECT
  enriched.*,
  COALESCE(calculated_layers.layers, 1) AS layers
FROM enriched
LEFT JOIN calculated_layers
  ON enriched.date_actual = calculated_layers.date_actual
    AND enriched.full_name = calculated_layers.employee
    AND enriched.employment_status IS NOT NULL
WHERE enriched.employment_status IS NOT NULL
