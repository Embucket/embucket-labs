WITH source AS (
  SELECT *
  FROM {{ source('snapshots','workday_employee_mapping_snapshots') }}
  
),

renamed AS (

  SELECT
    employee_id::NUMBER AS employee_id,
    employment_history_status::VARCHAR AS employment_history_status,
    employee_status_date::DATE AS employee_status_date,
    cost_center::VARCHAR AS cost_center,
    last_name::VARCHAR AS last_name,
    first_name::VARCHAR AS first_name,
    region::VARCHAR AS region,
    hire_date::DATE AS hire_date,
    country::VARCHAR AS country,
    greenhouse_candidate_id::NUMBER AS greenhouse_candidate_id,
    gender::VARCHAR AS gender,
    job_role::VARCHAR AS job_role,
    gender_dropdown::VARCHAR AS gender_dropdown,
    date_of_birth::DATE AS date_of_birth,
    job_grade::NUMBER AS job_grade,
    pay_frequency::FLOAT AS pay_frequency,
    age::NUMBER AS age,
    jobtitle_speciality_single_select::VARCHAR AS jobtitle_speciality_single_select,
    ethnicity::VARCHAR AS ethnicity,
    jobtitle_speciality_multi_select::VARCHAR AS jobtitle_speciality_multi_select,
    gitlab_username::VARCHAR AS gitlab_username,
    sales_geo_differential::VARCHAR AS sales_geo_differential,
    locality::VARCHAR AS locality,
    termination_date::DATE AS termination_date,
    nationality::VARCHAR AS nationality,
    _fivetran_synced::TIMESTAMP AS uploaded_at,
    dbt_valid_from::TIMESTAMP AS valid_from,
    dbt_valid_to::TIMESTAMP AS valid_to,
    IFF(dbt_valid_to IS NULL,TRUE,FALSE) AS is_current
  FROM source

)

SELECT *
FROM renamed
