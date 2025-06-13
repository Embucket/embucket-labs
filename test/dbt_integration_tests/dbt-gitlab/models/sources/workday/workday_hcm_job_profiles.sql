WITH job_profile AS (

  SELECT *
  FROM {{ ref('workday_hcm_job_profile_source') }}


),

job_family_job_profile AS (

  SELECT *
  FROM {{ ref('workday_hcm_job_family_job_profile_source') }}

),

job_family AS (

  SELECT *
  FROM {{ ref('workday_hcm_job_family_source') }}

),

final AS (
  SELECT
    job_profile.job_profile_code                 AS job_code,
    job_profile.title                            AS job_profile,
    job_profile.job_profile_id                   AS job_workday_id,
    job_profile.management_level                 AS management_level,
    REPLACE(job_profile.level, 'Job_Level_', '') AS job_level,
    job_family.job_family_code                   AS job_family,
    job_profile.is_inactive                      AS is_inactive,
    current_date                                 AS report_effective_date
  FROM job_profile
  LEFT JOIN job_family_job_profile
    ON job_profile.job_profile_id = job_family_job_profile.job_profile_id
  LEFT JOIN job_family
    ON job_family_job_profile.job_family_id = job_family.id
)

SELECT *
FROM final
