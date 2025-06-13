WITH builds as (

  SELECT *
  FROM {{ ref('dim_ci_build') }}

),

ci_stages as (

  SELECT *
  FROM {{ ref('dim_ci_stage') }}

),

pipelines as (

  SELECT *
  FROM {{ ref('dim_ci_pipeline') }}

),

finished_pipelines as (

  SELECT
    pipelines.dim_ci_pipeline_id,
    pipelines.finished_at::DATE                                                                                                     AS pipeline_finished_date,
    pipelines.status                                                                                                                AS pipeline_status,
    pipelines.ref                                                                                                                   AS pipeline_ref,
    pipelines.pipeline_schedule_id,
    pipelines.ci_pipeline_source                                                                                                    AS pipeline_source,
    pipelines.started_at                                                                                                            AS pipeline_started_at,
    MAX(builds.finished_at)                                                                                                         AS last_job_finished_at,
    TIMESTAMPDIFF(SECOND,pipelines.started_at, last_job_finished_at)                                                                AS duration_seconds_diff,
    pipelines.ci_pipeline_duration_in_s,
    IFF(duration_seconds_diff > pipelines.ci_pipeline_duration_in_s, pipelines.ci_pipeline_duration_in_s, duration_seconds_diff)    AS duration_seconds_adjusted,
    duration_seconds_adjusted / 60::FLOAT                                                                                           AS duration_minutes_adjusted
  FROM builds
  JOIN ci_stages
    ON ci_stages.dim_ci_stage_id = builds.dim_ci_stage_id
  JOIN pipelines
    ON ci_stages.dim_ci_pipeline_id = pipelines.dim_ci_pipeline_id
  WHERE pipelines.dim_project_id = 278964
    AND pipelines.finished_at IS NOT NULL
    AND pipelines.ci_pipeline_source != 'parent_pipeline'
    AND pipelines.status IN ('success', 'failed')
    AND builds.ci_build_type != 'Ci::Bridge'
  GROUP BY
    pipelines.dim_ci_pipeline_id,
    pipelines.finished_at::DATE,
    pipelines.status,
    pipelines.ref,
    pipelines.ci_pipeline_source,
    pipelines.started_at,
    pipelines.pipeline_schedule_id,
    pipelines.ci_pipeline_source,
    pipelines.started_at,
    pipelines.finished_at,
    pipelines.ci_pipeline_duration_in_s
  
)

SELECT *
FROM finished_pipelines
