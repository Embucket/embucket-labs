WITH source AS (

  SELECT *
  FROM {{ source('workday','job_profiles') }}
  WHERE NOT _fivetran_deleted


),

renamed AS (

  SELECT
    job_code::VARCHAR                       AS job_code,
    job_profile::VARCHAR                    AS job_profile,
    job_family::VARCHAR                     AS job_family,
    management_level::VARCHAR               AS management_level,
    job_level::VARCHAR                      AS job_level,
    IFF(inactive::BOOLEAN = 0, TRUE, FALSE) AS is_job_profile_active,
    job_workday_id::VARCHAR                 AS job_workday_id
  FROM source

)

SELECT *
FROM renamed
