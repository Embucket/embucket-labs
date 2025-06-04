WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'job_profiles_historical') }}

),

final AS (
  SELECT
    report_effective_date::DATE             AS report_effective_date,
    job_workday_id,
    job_code,
    job_profile,
    management_level,
    job_level::FLOAT                        AS job_level,
    job_family,
    IFF(true::BOOLEAN = 1, FALSE, TRUE) AS is_job_profile_active
  FROM source
)

SELECT *
FROM final
