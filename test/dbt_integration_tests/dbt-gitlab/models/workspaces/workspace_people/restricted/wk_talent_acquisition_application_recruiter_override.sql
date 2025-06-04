WITH greenhouse_applications_jobs_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_jobs_source') }}
),

greenhouse_job_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_job_custom_fields_source') }}
),

greenhouse_applications_source AS (
  SELECT *
  FROM {{ ref('greenhouse_applications_source') }}
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

job_custom AS (
  SELECT
    job_id AS job_custom_id,
    MAX(CASE job_custom_field
      WHEN 'Job Grade'
        THEN job_custom_field_display_value
    END)   AS job_grade
  FROM greenhouse_job_custom_fields_source
  GROUP BY 1
),

app AS (
  SELECT
    greenhouse_applications_source.application_id,
    greenhouse_applications_source.candidate_id,
    xf.job_id,
    greenhouse_applications_source.application_recruiter                                                      AS app_recruiter,
    --,job_grade
    IFF(job_custom.job_grade IN ('10', '11', '12', '13', '14', '15'), 'Dir+', '') AS app_recruiter_map_override
  FROM greenhouse_applications_source
  INNER JOIN xf
    ON greenhouse_applications_source.application_id = xf.application_id
      AND 1 = xf.job_id_rank
  LEFT JOIN job_custom ON xf.job_id = job_custom.job_custom_id
)

SELECT *
FROM app
