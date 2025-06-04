WITH job_profiles_snapshots_source AS (
  SELECT *
  FROM {{ ref('job_profiles_snapshots_source') }}
),

job_profiles_historical_source AS (
  SELECT *
  FROM {{ ref('job_profiles_historical_source') }}
),

max_hist_date_cte AS (
  SELECT MAX(job_profiles_historical_source.report_effective_date) AS max_hist_date
  FROM job_profiles_historical_source
),

job_profiles_stage AS (
  SELECT
    job_profiles_snapshots_source.report_effective_date,
    job_workday_id,
    job_code,
    job_profile,
    management_level,
    job_level,
    job_family,
    IFF(inactive::BOOLEAN = 1, FALSE, TRUE) AS is_job_profile_active
  FROM job_profiles_snapshots_source
  LEFT JOIN max_hist_date_cte 
    ON job_profiles_snapshots_source.report_effective_date > max_hist_date_cte.max_hist_date

  UNION

  SELECT
    report_effective_date,
    job_workday_id,
    job_code,
    job_profile,
    management_level,
    job_level,
    job_family,
    is_job_profile_active
  FROM job_profiles_historical_source
),

final AS (
  SELECT
    job_workday_id,
    ROW_NUMBER() OVER (
      PARTITION BY job_workday_id ORDER BY report_effective_date ASC
    )                                                             AS record_rank_asc,
    ROW_NUMBER() OVER (
      PARTITION BY job_workday_id ORDER BY report_effective_date DESC
    )                                                             AS record_rank_desc,
    IFF(record_rank_asc = 1, '1900-01-01', report_effective_date) AS valid_from,
    COALESCE(LAG(report_effective_date) OVER (
      PARTITION BY job_workday_id ORDER BY report_effective_date DESC
    ), '2099-01-01')                                              AS valid_to,
    job_code,
    job_profile,
    management_level,
    job_level                                                     AS job_level,
    job_family,
    is_job_profile_active                                         AS is_job_profile_active
  FROM job_profiles_stage
)

SELECT
  job_workday_id,
  job_code,
  job_profile,
  management_level,
  job_level,
  job_family,
  is_job_profile_active,
  valid_from,
  valid_to
FROM final
