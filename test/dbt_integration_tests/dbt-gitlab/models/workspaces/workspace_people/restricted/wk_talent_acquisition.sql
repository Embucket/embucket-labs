WITH greenhouse_jobs_source AS (
  SELECT *
  FROM {{ ref('greenhouse_jobs_source') }}
),

greenhouse_openings_source AS (
  SELECT *
  FROM {{ ref('greenhouse_openings_source') }}
),

greenhouse_application_stages_source AS (
  SELECT *
  FROM {{ ref('greenhouse_application_stages_source') }}
),

greenhouse_applications_jobs_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_jobs_source') }}
),

greenhouse_offers_source AS (
  SELECT *
  FROM {{ ref('greenhouse_offers_source') }}
),

greenhouse_recruiting_xf AS (
  SELECT *
  FROM {{ ref('greenhouse_recruiting_xf') }}
),

greenhouse_hires AS (
  SELECT *
  FROM {{ ref('greenhouse_hires') }}
),

greenhouse_applications_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_source') }}
),

candidates_raw AS (
  SELECT *
  FROM {{ source('greenhouse', 'candidates') }}
),

job AS (
  SELECT
    'Requisition'                                                                         AS type,
    job_id                                                                                AS job_id,
    NULL                                                                                  AS application_id,
    DATE_TRUNC('day', IFF(job_opened_at < job_created_at, job_opened_at, job_created_at)) AS start_date,
    COALESCE(job_closed_at, '2099-01-01')                                                 AS end_date,
    job_id                                                                                AS id,
    ''                                                                                    AS status,
    NULL                                                                                  AS status_id
  FROM greenhouse_jobs_source
),

openings AS (
  SELECT
    'Opening'                             AS type,
    job_id                                AS job_id,
    hired_application_id                  AS application_id,
    DATE_TRUNC('day', job_opened_at)      AS start_date,
    --,date_trunc('day', IFF(job_opened_at > job_opening_created_at, job_opened_at, job_opening_created_at)) AS start_date
    COALESCE(job_closed_at, '2099-01-01') AS end_date,
    job_opening_id                        AS id,
    ''                                    AS status_id,
    NULL                                  AS status_id
  FROM greenhouse_openings_source
  WHERE opening_id IS NOT NULL
    AND job_opened_at IS NOT NULL
),

stages_base AS (
  SELECT
    'Application'                                                 AS type,
    xf.job_id                                                     AS job_id,
    stg.application_id,
    stg.stage_entered_on                                          AS start_date,
    stg.stage_exited_on                                           AS end_date,
    stg.application_id                                            AS id,
    COALESCE(stg.application_stage_name, stg.stage_name_modified) AS status,
    IFF(stg.application_stage_name = 'Hired', 1000, stg.stage_id) AS status_id
  FROM greenhouse_application_stages_source AS stg
  INNER JOIN greenhouse_applications_jobs_source AS xf ON stg.application_id = xf.application_id
  LEFT JOIN greenhouse_recruiting_xf AS recruiting_xf ON stg.application_id = recruiting_xf.application_id
  WHERE stg.stage_entered_on IS NOT NULL
    AND recruiting_xf.application_id IS NOT NULL


),

stages_apply AS (
  SELECT
    'Application'                       AS type,
    xf.job_id,
    xf.application_id,
    DATE_TRUNC('day', application_date) AS start_date,
    NULL                                AS end_date,
    xf.application_id                   AS id,
    'Application Submitted'             AS status,
    -1                                  AS status_id
  FROM greenhouse_recruiting_xf AS xf
  WHERE job_id IS NOT NULL
),

stages_reject AS (
  SELECT
    'Application'                                                                                                 AS type,
    xf.job_id,
    xf.application_id,
    //,max(coalesce(stage_exited_on, xf.rejected_date)) AS start_date
    MAX(DATEADD('second', -2, DATEADD('day', 1, DATE_TRUNC('day', COALESCE(stage_exited_on, xf.rejected_date))))) AS start_date,
    NULL                                                                                                          AS end_date,
    xf.application_id                                                                                             AS id,
    'Rejected'                                                                                                    AS status,
    100                                                                                                           AS status_id
  FROM greenhouse_recruiting_xf AS xf
  LEFT JOIN greenhouse_application_stages_source AS stg ON xf.application_id = stg.application_id
  WHERE xf.job_id IS NOT NULL
    AND xf.application_status = 'rejected'
  GROUP BY
    1,
    2,
    3
),

stages_start AS (
  SELECT
    'Application'                                                                                 AS type,
    recruiting_xf.job_id,
    recruiting_xf.application_id,
    COALESCE(hires.hire_date_mod::TIMESTAMP, recruiting_xf.candidate_target_hire_date::TIMESTAMP) AS start_date,
    COALESCE(hires.hire_date_mod::TIMESTAMP, recruiting_xf.candidate_target_hire_date::TIMESTAMP) AS end_date,
    recruiting_xf.application_id                                                                  AS id,
    'Started'                                                                                     AS status,
    10000                                                                                         AS status_id
  FROM greenhouse_recruiting_xf AS recruiting_xf
  LEFT JOIN greenhouse_hires AS hires ON recruiting_xf.application_id = hires.application_id
  INNER JOIN stages_base
    ON recruiting_xf.application_id = stages_base.application_id
      AND recruiting_xf.job_id = stages_base.job_id
      AND 'Hired' = stages_base.status
  WHERE offer_status = 'accepted'
    AND recruiting_xf.application_status = 'hired'
    AND COALESCE(hires.hire_date_mod::TIMESTAMP, recruiting_xf.candidate_target_hire_date::TIMESTAMP) IS NOT NULL
),

stages AS (
  SELECT *
  FROM stages_base

  UNION ALL

  SELECT *
  FROM stages_apply

  UNION ALL

  SELECT *
  FROM stages_reject
),

app_stages_final AS (
  SELECT
    stages.type,
    stages.job_id,
    stages.application_id,
    stages.start_date,
    COALESCE(LEAD(stages.start_date) OVER (
      PARTITION BY stages.application_id ORDER BY stages.start_date ASC
    ), IFF(job.end_date < stages.start_date, DATEADD('second', -1, DATEADD('day', 1, DATE_TRUNC('day', stages.start_date))), job.end_date), '2099-01-01') AS end_date,
    stages.id,
    stages.status,
    stages.status_id
  FROM stages
  LEFT JOIN job ON stages.job_id = job.job_id
  ORDER BY
    stages.application_id,
    stages.start_date
),

offer AS (
  SELECT *
  FROM greenhouse_offers_source QUALIFY ROW_NUMBER() OVER (
    PARTITION BY application_id ORDER BY created_at DESC
  ) = 1
),

candidates AS (
  SELECT
    'Candidate'            AS type,
    NULL                   AS job_id,
    NULL                   AS application_id,
    created_at             AS start_date,
    '2099-01-01'::DATETIME AS end_date,
    id,
    ''                     AS status,
    NULL                   AS status_id
  FROM candidates_raw
),

intermediate AS (
  SELECT *
  FROM job

  UNION ALL

  SELECT *
  FROM openings

  UNION ALL

  SELECT *
  FROM app_stages_final

  UNION ALL

  SELECT *
  FROM stages_start

  UNION ALL

  SELECT *
  FROM candidates
),

final AS (
  SELECT
    intermediate.type,
    intermediate.job_id,
    IFF(intermediate.type = 'Opening', intermediate.id, openings.id)        AS job_opening_id,
    IFF(intermediate.type = 'Candidate', intermediate.id, app.candidate_id) AS candidate_id,
    intermediate.application_id,
    offer.offer_id,
    intermediate.start_date                                                 AS start_datetime,
    intermediate.end_date                                                   AS end_datetime,
    intermediate.start_date::DATE                                           AS start_date,
    intermediate.end_date::DATE                                             AS end_date,
    intermediate.id,
    intermediate.status,
    intermediate.status_id
  FROM intermediate
  LEFT JOIN offer ON intermediate.application_id = offer.application_id
  LEFT JOIN greenhouse_applications_source AS app ON intermediate.application_id = app.application_id
  LEFT JOIN openings
    ON intermediate.type = 'Application'
      AND intermediate.job_id = openings.job_id
      AND intermediate.application_id = openings.application_id
  WHERE start_datetime < end_datetime
    AND
    IFF(
      intermediate.type = 'Application'
      AND COALESCE(app.prospect, 'f') = 't', 0, 1) = 1 QUALIFY MAX(end_datetime)
    OVER (
      PARTITION BY
        intermediate.type,
        intermediate.id
      ORDER BY end_datetime DESC
    )
  >= DATE_TRUNC('year', DATEADD('year', -1, CURRENT_DATE)
  )
)

SELECT
  *,
  MAX(final.start_date) OVER (
    PARTITION BY
      final.type,
      final.id
    ORDER BY final.start_datetime DESC
  )                                               AS max_start_date,
  MAX(final.end_date) OVER (
    PARTITION BY
      final.type,
      final.id
    ORDER BY final.end_datetime DESC
  )                                               AS max_end_date,
  MIN(final.start_date) OVER (
    PARTITION BY
      final.type,
      final.id
    ORDER BY final.start_datetime ASC
  )                                               AS min_start_date,
  IFF(ROW_NUMBER() OVER (
    PARTITION BY
      final.type,
      final.id ORDER BY final.start_datetime DESC,
    final.end_datetime DESC
  ) = 1, 'Y', 'N')                                AS is_max_record,
  IFF(SUM(IFF(final.type = 'Application' AND final.status != 'Rejected', 1, 0)) OVER (
    PARTITION BY
      final.type,
      final.id
    ORDER BY IFF(final.status = 'Hired', 1, 0) DESC, final.start_datetime DESC
  ) = 1 AND final.type = 'Application', 'Y', 'N') AS is_max_hire_progress_step
FROM final
