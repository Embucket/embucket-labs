WITH source AS (

  SELECT *
  FROM {{ ref ('blended_employee_mapping_source') }}
  WHERE sort_order = 1

),

intermediate AS (

  SELECT
    employee_number,
    employee_id,
    first_name,
    last_name,
    hire_date,
    termination_date,
    first_inactive_date,
    job_role,
    job_grade,
    cost_center,
    CASE
      WHEN jobtitle_speciality_multi_select IS NULL
        AND jobtitle_speciality_single_select IS NULL
        THEN NULL
      WHEN jobtitle_speciality_single_select IS NULL
        THEN jobtitle_speciality_multi_select
      WHEN jobtitle_speciality_multi_select IS NULL
        THEN jobtitle_speciality_single_select
      ELSE jobtitle_speciality_single_select || ',' || jobtitle_speciality_multi_select
    END AS jobtitle_speciality,
    gitlab_username,
    pay_frequency,
    sales_geo_differential,
    region,
    DATE_TRUNC('day', uploaded_at) AS effective_date,
    {{ dbt_utils.generate_surrogate_key(['employee_id', 'job_role', 'job_grade',
                                'cost_center', 'jobtitle_speciality', 
                                'gitlab_username', 'pay_frequency', 
                                'sales_geo_differential']) }} AS unique_key,
    LAG(unique_key, 1, '')
      OVER (PARTITION BY employee_id ORDER BY uploaded_at) AS lag_unique_key,
    CONDITIONAL_TRUE_EVENT(unique_key != lag_unique_key)
      OVER ( PARTITION BY employee_id ORDER BY uploaded_at) AS unique_key_group 
  FROM source

),

filtered AS (
  
  SELECT *
  FROM intermediate
  QUALIFY ROW_NUMBER() OVER (PARTITION BY unique_key,unique_key_group
    ORDER BY DATE_TRUNC('day', effective_date) ASC, DATE_TRUNC('hour', effective_date) DESC) = 1
  
),

final AS (

  SELECT
    *,
    LEAD(DATEADD('day', -1, DATE_TRUNC('day', effective_date))) OVER (PARTITION BY employee_number
      ORDER BY effective_date) AS next_effective_date
  FROM filtered
  WHERE effective_date >= '2020-02-27'  --1st day we started capturing job role

)

SELECT *
FROM final
