{{
  config({
    "materialized": "incremental",
    "unique_key": "ci_build_id",
    "tmp_relation_type": "table",
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH ci_builds AS (

  SELECT *
  FROM {{ ref('prep_ci_build') }}
  {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})

  {% endif %}

),

projects AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_projects') }}

),

namespace_lineage AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_lineage') }}

),

gitlab_subscriptions AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base') }}

),

secure_ci_builds AS (

  SELECT
    ci_build_id,
    dim_project_id       AS ci_build_project_id,
    dim_ci_runner_id     AS ci_build_runner_id,
    dim_user_id          AS ci_build_user_id,
    dim_ci_stage_id      AS ci_build_stage_id,
    ci_build_status      AS status,
    finished_at,
    created_at,
    updated_at,
    started_at,
    coverage,
    commit_id            AS ci_build_commit_id,
    ci_build_name,
    options,
    allow_failure,
    stage,
    trigger_request_id   AS ci_build_trigger_request_id,
    stage_idx,
    tag,
    ref,
    ci_build_type        AS type,
    ci_build_description AS description,
    erased_by_id         AS ci_build_erased_by_id,
    erased_at            AS ci_build_erased_at,
    artifacts_expire_at  AS ci_build_artifacts_expire_at,
    environment,
    queued_at            AS ci_build_queued_at,
    lock_version,
    coverage_regex,
    auto_canceled_by_id  AS ci_build_auto_canceled_by_id,
    retried,
    protected,
    failure_reason_id,
    scheduled_at         AS ci_build_scheduled_at,
    upstream_pipeline_id,
    secure_ci_build_type AS secure_ci_job_type
  FROM ci_builds
  WHERE secure_ci_build_type IS NOT NULL
),

joined AS (

  SELECT
    secure_ci_builds.*,
    namespace_lineage.namespace_is_internal AS is_internal_job,
    namespace_lineage.ultimate_parent_id,
    namespace_lineage.ultimate_parent_plan_id,
    namespace_lineage.ultimate_parent_plan_title,
    namespace_lineage.ultimate_parent_plan_is_paid,

    CASE
      WHEN gitlab_subscriptions.is_trial
        THEN 'trial'
      ELSE COALESCE(gitlab_subscriptions.plan_id, 34)::VARCHAR
    END                                     AS plan_id_at_job_creation
  FROM secure_ci_builds
  LEFT JOIN projects
    ON secure_ci_builds.ci_build_project_id = projects.project_id
  LEFT JOIN namespace_lineage
    ON projects.namespace_id = namespace_lineage.namespace_id
  LEFT JOIN gitlab_subscriptions
    ON namespace_lineage.ultimate_parent_id = gitlab_subscriptions.namespace_id
      AND secure_ci_builds.created_at BETWEEN gitlab_subscriptions.valid_from AND {{ coalesce_to_infinity("gitlab_subscriptions.valid_to") }}

)

SELECT *
FROM secure_ci_builds
