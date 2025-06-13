WITH source AS (

  SELECT *
  FROM {{ source('snapshots', 'job_profiles_snapshots') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    report_effective_date::DATE             AS report_effective_date,
    job_code::VARCHAR                       AS job_code,
    job_profile::VARCHAR                    AS job_profile,
    job_family::VARCHAR                     AS job_family,
    management_level::VARCHAR               AS management_level,
    job_level::FLOAT                        AS job_level,
    inactive::BOOLEAN                       AS inactive,
    IFF(inactive::BOOLEAN = 0, TRUE, FALSE) AS is_job_profile_active,
    dbt_valid_from::TIMESTAMP               AS valid_from,
    dbt_valid_to::TIMESTAMP                 AS valid_to,
    job_workday_id::VARCHAR                 AS job_workday_id
  FROM source

)

SELECT *
FROM final
