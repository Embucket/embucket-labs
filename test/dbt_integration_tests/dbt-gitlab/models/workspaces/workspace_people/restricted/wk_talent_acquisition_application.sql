WITH greenhouse_applications_jobs_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_jobs_source') }}
)
,
greenhouse_applications_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_source') }}
),

greenhouse_stages_source AS (
  SELECT *
  FROM {{ ref('greenhouse_stages_source') }}
),

greenhouse_jobs_source AS (
  SELECT *
  FROM {{ ref('greenhouse_jobs_source') }}
),

greenhouse_application_stages_source AS (
  SELECT *
  FROM {{ ref('greenhouse_application_stages_source') }}
),

referrer AS (
  SELECT *
  FROM {{ ref('greenhouse_referrers_source') }}
),

users AS (
  SELECT *
  FROM {{ ref('greenhouse_users_source') }}
),

job_posts AS (
  SELECT *
  FROM {{ ref('greenhouse_job_posts_source') }}
),

rej_rsn AS (
  SELECT *
  FROM {{ ref('greenhouse_rejection_reasons_source') }}
),

greenhouse_offers_source AS (
  SELECT *
  FROM {{ ref('greenhouse_offers_source') }}
),

source AS (
  SELECT *
  FROM {{ ref('greenhouse_sources_source') }}
),

greenhouse_hires AS (
  SELECT *
  FROM {{ ref('greenhouse_hires') }}
),

greenhouse_recruiting_xf AS (
  SELECT *
  FROM {{ ref('greenhouse_recruiting_xf') }}
),

employee_directory_intermediate AS (
  SELECT *
  FROM {{ ref('employee_directory_intermediate') }}
),

bamboohr_id_employee_number_mapping AS (
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping') }}
),

xf AS (
  SELECT
    xf.application_id,
    xf.job_id,
    ROW_NUMBER() OVER (
      PARTITION BY xf.application_id ORDER BY xf.job_id DESC
    ) AS job_id_rank
  FROM greenhouse_applications_jobs_source AS xf
),

app AS (
  SELECT
    app.*,
    xf.job_id,
    xf.job_id_rank
  FROM greenhouse_applications_source AS app
  INNER JOIN xf ON app.application_id = xf.application_id
    AND 1 = xf.job_id_rank
),

hires AS (
  SELECT
    xf.job_id                                                     AS job_id,
    stg.application_id,
    COALESCE(stg.stage_name_modified, stg.application_stage_name) AS stage_name_modified,
    stg.stage_entered_on,
    stg.stage_exited_on
  FROM greenhouse_application_stages_source AS stg
  INNER JOIN greenhouse_applications_jobs_source AS xf ON stg.application_id = xf.application_id
  LEFT JOIN greenhouse_recruiting_xf AS recruiting_xf ON stg.application_id = recruiting_xf.application_id
  WHERE stg.stage_entered_on IS NOT NULL
    AND recruiting_xf.application_id IS NOT NULL
    AND stg.application_stage_name = 'Hired'
),

stages_reject AS (
  SELECT
    xf.job_id,
    xf.application_id,
    MAX(COALESCE(stg.stage_exited_on, xf.rejected_date)) AS reject_date
  FROM greenhouse_recruiting_xf AS xf
  LEFT JOIN greenhouse_application_stages_source AS stg ON xf.application_id = stg.application_id
  WHERE xf.job_id IS NOT NULL
    AND xf.application_status = 'rejected'
  GROUP BY
    1,
    2
),

stages_start AS (
  SELECT
    recruiting_xf.job_id,
    recruiting_xf.application_id,
    COALESCE(hires.hire_date_mod::TIMESTAMP, recruiting_xf.candidate_target_hire_date::TIMESTAMP) AS start_date
  FROM greenhouse_recruiting_xf AS recruiting_xf
  LEFT JOIN greenhouse_hires AS hires ON recruiting_xf.application_id = hires.application_id
  WHERE recruiting_xf.offer_status = 'accepted'
    AND recruiting_xf.application_status = 'hired'
),

external_start AS (
  SELECT
    edi.employee_id,
    edi.date_actual,
    map.greenhouse_candidate_id,
    1 AS external_hire
  FROM employee_directory_intermediate AS edi
  LEFT JOIN bamboohr_id_employee_number_mapping AS map ON edi.employee_id = map.employee_id
  WHERE edi.is_hire_date = 'True'
),

offer AS (
  SELECT *
  FROM greenhouse_offers_source QUALIFY ROW_NUMBER() OVER (
    PARTITION BY application_id ORDER BY created_at DESC
  ) = 1
)

SELECT DISTINCT
  app.application_id,
  app.candidate_id,
  app.job_id,
  app.job_post_id,
  DATE_TRUNC('day', app.applied_at)                                 AS applied_at,
  app.application_status,
  app.application_recruiter,
  app.application_recruiter_id,
  app.application_coordinator,
  app.application_coordinator_id,  
  source.source_name,
  source.source_type,
  app.prospect,
  IFF(
    source.source_type = 'Prospecting'
    AND source.source_name NOT IN ('SocialReferral'), referrer.referrer_name, NULL
  )                                                                 AS sourcer_name,
  referrer.referrer_name,
  u1.employee_id                                                    AS referrer_employee_id,
  app.rejected_by,
  u2.employee_id                                                    AS rejected_by_employee_id,
  rej_rsn.rejection_reason_name,
  rej_rsn.rejection_reason_type,
  COALESCE(rej.reject_date, app.rejected_at)                        AS rejected_at,
  hires.stage_entered_on                                            AS hired_at,
  strt.start_date                                                   AS started_at,
  IFF(external_start.greenhouse_candidate_id IS NOT NULL, 'Y', 'N') AS external_start,
  app.created_at,
  app.last_updated_at
FROM app
LEFT JOIN source ON app.source_id = source.source_id
LEFT JOIN greenhouse_stages_source ON app.stage_id = greenhouse_stages_source.stage_id
LEFT JOIN hires
  ON app.application_id = hires.application_id
    AND app.job_id = hires.job_id
    AND 'Hired' = hires.stage_name_modified
LEFT JOIN referrer ON app.referrer_id = referrer.referrer_id
LEFT JOIN users AS u1 ON referrer.user_id = u1.user_id
LEFT JOIN users AS u2 ON app.rejected_by_id = u2.user_id
LEFT JOIN job_posts
  ON app.job_id = job_posts.job_id
    AND app.job_post_id = job_posts.job_post_id
LEFT JOIN rej_rsn ON app.rejection_reason_id = rej_rsn.rejection_reason_id
LEFT JOIN
  stages_reject
    AS rej
  ON app.application_id = rej.application_id
    AND app.job_id = rej.job_id
    AND app.application_status = 'rejected'
LEFT JOIN
  stages_start
    AS strt
  ON app.application_id = strt.application_id
    AND app.job_id = strt.job_id
LEFT JOIN offer ON app.application_id = offer.application_id
LEFT JOIN external_start
  ON app.candidate_id = external_start.greenhouse_candidate_id
    AND strt.start_date::DATE = external_start.date_actual
WHERE app.prospect = 'f'
