WITH pipelines AS (

  SELECT *
  FROM {{ ref('dim_ci_pipeline') }}
  
),

ci_stages AS (

  SELECT *
  FROM {{ ref('dim_ci_stage') }}

),

ci_builds AS (

  SELECT *
  FROM {{ ref('dim_ci_build') }}

),

ci_sources_pipelines AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_sources_pipelines') }}

),

date_details AS (

  SELECT *
  FROM {{ ref('prep_date') }}

),

failed_mr_pipelines AS (

  SELECT 
    dim_ci_pipeline_id AS ci_pipeline_id,
    created_at,
    started_at,
    finished_at
  FROM pipelines
  WHERE dim_project_id = 278964
  AND status = 'failed'
  AND PIPELINE_SCHEDULE_ID IS NULL
  AND ref LIKE 'refs/merge-requests%'
  AND ci_pipeline_source != 'parent_pipeline'
  AND dim_user_id != 1614863

),

failed_mr_pipelines_with_failed_builds AS (

  SELECT
    failed_mr_pipelines.ci_pipeline_id,
    failed_mr_pipelines.created_at AS pipeline_created_at,
    date_trunc('month', failed_mr_pipelines.created_at) as pipeline_month,
    failed_mr_pipelines.started_at AS pipeline_started_at,
    failed_mr_pipelines.finished_at AS pipeline_finished_at,
    ci_builds.ci_build_name,
    ci_builds.dim_ci_build_id AS ci_build_id,
    ci_builds.started_at AS ci_build_started_at,
    ci_builds.finished_at AS ci_build_finished_at,
    ci_builds.retried,
    ci_stages.ci_stage_name AS ci_stage_name,
    'parent' AS pipeline_failure_type,
    failed_mr_pipelines.ci_pipeline_id AS failed_pipeline_id,
    CASE
      WHEN ci_builds.ci_build_name IN ('docs lint', 'docs-lint links') 
        THEN 'docs'
      WHEN ci_builds.ci_build_name = 'retrieve-tests-metadata' 
        THEN 'code'
      WHEN ci_builds.ci_build_name IN ('review-deploy', 'start-review-app-pipeline') 
        THEN 'review-app'
      WHEN ci_builds.ci_build_name IN ('package-and-qa', 'package-and-qa-ff-enabled', 'package-and-qa-ff-disabled', 'e2e:package-and-test', 'e2e:package-and-test-ee') 
        THEN 'e2e'
      ELSE 'unknown'
    END AS pipeline_type
  FROM failed_mr_pipelines
  JOIN ci_stages ON ci_stages.dim_ci_pipeline_id = failed_mr_pipelines.ci_pipeline_id
  JOIN ci_builds ON ci_builds.dim_ci_stage_id = ci_stages.dim_ci_stage_id AND ci_builds.ci_build_status = 'failed' AND ci_builds.allow_failure = 'false'
  
  UNION

  SELECT
    failed_mr_pipelines.ci_pipeline_id,
    failed_mr_pipelines.created_at AS pipeline_created_at,
    date_trunc('month', failed_mr_pipelines.created_at) as pipeline_month,
    failed_mr_pipelines.started_at AS pipeline_started_at,
    failed_mr_pipelines.finished_at AS pipeline_finished_at,
    ci_builds.ci_build_name,
    ci_builds.dim_ci_build_id AS ci_build_id,
    ci_builds.started_at AS ci_build_started_at,
    ci_builds.finished_at AS ci_build_finished_at,
    ci_builds.retried,
    ci_stages.ci_stage_name AS ci_stage_name,
    'child' AS pipeline_failure_type,
    pipelines.ci_pipeline_id AS failed_pipeline_id,
    CASE
      WHEN ci_builds.ci_build_name IN ('docs lint', 'docs-lint links') 
        THEN 'docs'
      WHEN ci_builds.ci_build_name = 'retrieve-tests-metadata' 
        THEN 'code'
      WHEN ci_builds.ci_build_name IN ('review-deploy', 'start-review-app-pipeline') 
        THEN 'review-app'
      WHEN ci_builds.ci_build_name IN ('package-and-qa', 'package-and-qa-ff-enabled', 'package-and-qa-ff-disabled', 'e2e:package-and-test', 'e2e:package-and-test-ee') 
        THEN 'e2e'
      ELSE 'unknown'
    END AS pipeline_type
  FROM failed_mr_pipelines
  JOIN ci_sources_pipelines ON ci_sources_pipelines.SOURCE_PIPELINE_ID = failed_mr_pipelines.ci_pipeline_id
  JOIN pipelines ON pipelines.ci_pipeline_id = ci_sources_pipelines.pipeline_id
  JOIN ci_stages ON ci_stages.dim_ci_pipeline_id = pipelines.ci_pipeline_id
  JOIN ci_builds ON ci_builds.dim_ci_stage_id = ci_stages.dim_ci_stage_id AND ci_builds.ci_build_status = 'failed' AND ci_builds.allow_failure = 'false'

), 

earliest_failed_build AS (

  SELECT
    ci_pipeline_id,
    MIN(ci_build_finished_at) AS first_failure_ts
  FROM failed_mr_pipelines_with_failed_builds
  GROUP BY 1
  
),

base AS (

  SELECT
    failed_mr_pipelines_with_failed_builds.*,
    DATEDIFF('seconds', failed_mr_pipelines.created_at, first_failure_ts) / 60 as minutes_to_failure,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(failed_mr_pipelines_with_failed_builds.ci_build_name, '-?[0-9]+(/[0-9]+)?'),
              ' pull-cache'),
            ' pull-push-cache'),
          '-as-if-foss'),
        ' as-if-foss') AS trimmed_build_name,
    CASE
      WHEN trimmed_build_name LIKE 'review-qa%' OR trimmed_build_name = 'qa:selectors' OR trimmed_build_name = 'e2e:package-and-test' OR trimmed_build_name = 'e2e:package-and-test-ee' 
        THEN 'end-to-end'
      WHEN trimmed_build_name LIKE 'rspec%' OR trimmed_build_name LIKE 'jest%'
        THEN 'development tests' 
      WHEN trimmed_build_name LIKE 'docs%'
        THEN 'docs'
      ELSE 'engineering productivity'
    END AS failure_segment
  FROM failed_mr_pipelines
  JOIN earliest_failed_build ON earliest_failed_build.ci_pipeline_id = failed_mr_pipelines.ci_pipeline_id
  JOIN failed_mr_pipelines_with_failed_builds ON earliest_failed_build.first_failure_ts = failed_mr_pipelines_with_failed_builds.ci_build_finished_at
  WHERE minutes_to_failure <= 180
  
),

final AS (

  SELECT DISTINCT
    DATE_TRUNC('month', date_actual) AS month,
    base.*
  FROM date_details
  JOIN base ON DATE_TRUNC('month', date_actual) = base.pipeline_month
  WHERE DATE_TRUNC('month', date_actual) <= DATE_TRUNC('month', CURRENT_DATE)
  AND DATE_TRUNC('month', date_actual) > DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE))

)

SELECT *
FROM final
