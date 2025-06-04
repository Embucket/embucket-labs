WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','job_profile') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    id::VARCHAR                             AS job_profile_id,
    compensation_grade_id::VARCHAR          AS compensation_grade_id,
    job_category_id::VARCHAR                AS job_category_id,
    IFF(inactive::BOOLEAN = 0, TRUE, FALSE) AS is_job_profile_active,
    inactive::BOOLEAN                       AS is_inactive,
    management_level::VARCHAR               AS management_level,
    level::VARCHAR                          AS level,
    title::VARCHAR                          AS title,
    job_profile_code::VARCHAR               AS job_profile_code,
    current_date                            AS effective_date
    
  FROM source

)

SELECT *
FROM final
