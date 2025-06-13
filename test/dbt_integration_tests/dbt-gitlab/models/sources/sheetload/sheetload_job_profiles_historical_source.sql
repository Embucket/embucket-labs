WITH source AS (

  SELECT *
  FROM {{ source('sheetload','job_profiles_historical') }}

),

renamed AS (

  SELECT
    job_code::VARCHAR                           AS job_code,
    job_profile::VARCHAR                        AS job_profile,
    job_family::VARCHAR                         AS job_family,
    management_level::VARCHAR                   AS management_level,
    job_level::VARCHAR                          AS job_level,
    IFF(inactive::VARCHAR = 'Yes', TRUE, FALSE) AS is_job_profile_active,
    report_effective_date::DATE                 AS report_effective_date,
    job_workday_id::VARCHAR                     AS job_workday_id

  FROM source

)

SELECT *
FROM renamed
