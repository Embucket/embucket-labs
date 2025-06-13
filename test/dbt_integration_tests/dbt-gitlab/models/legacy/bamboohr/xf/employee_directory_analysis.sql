{{ config({
    "materialized":"table",
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
    })
}}

WITH employee_directory_intermediate AS (

  SELECT * FROM {{ ref('employee_directory_intermediate') }}

),


cleaned AS (

  SELECT
    date_actual,
    employee_id,
    reports_to,
    reports_to_id,
    full_name,
    work_email,
    gitlab_username,
    region_modified AS region,
    sales_geo_differential,
    jobtitle_speciality,
    job_role_modified,
    is_hire_date,
    IFF(date_actual > CURRENT_DATE, 'false', is_termination_date) AS is_termination_date,
    hire_date,
    cost_center,
    layers,
    job_title,
    COALESCE(location_factor, hire_location_factor) AS location_factor,
    CASE --the below case when statement is also used in bamboohr_job_info;
      WHEN division = 'Alliances' THEN 'Alliances'
      WHEN division = 'Customer Support' THEN 'Customer Support'
      WHEN division = 'Customer Service' THEN 'Customer Success'
      WHEN department = 'Data & Analytics' THEN 'Business Operations'
      ELSE NULLIF(department, '')
    END AS department,
    CASE
      WHEN department = 'Meltano' THEN 'Meltano'
      WHEN division = 'Employee' THEN NULL
      WHEN division = 'Contractor ' THEN NULL
      WHEN division = 'Alliances' THEN 'Sales'
      WHEN division = 'Customer Support' THEN 'Engineering'
      WHEN division = 'Customer Service' THEN 'Sales'
      ELSE NULLIF(division, '')
    END AS division,
    division_mapped_current,
    CASE
      WHEN date_actual < '2020-06-09'
        THEN FALSE
      WHEN date_actual >= '2020-06-09' AND LOWER(sales_geo_differential) = 'n/a - comp calc'
        THEN FALSE
      ELSE TRUE
    END AS exclude_from_location_factor
  FROM employee_directory_intermediate

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['date_actual', 'employee_id']) }} AS unique_key,
    *
  FROM cleaned

)

SELECT DISTINCT *
FROM final
