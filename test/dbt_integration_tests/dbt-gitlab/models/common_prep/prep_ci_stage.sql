{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_project', 'prep_project'),
    ('prep_ci_pipeline', 'prep_ci_pipeline'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('prep_date', 'prep_date')
]) }},

ci_stages AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_stages_source') }}
  WHERE created_at IS NOT NULL

),

joined AS (

  SELECT

    -- SURROGATE KEY
    {{ dbt_utils.generate_surrogate_key(['ci_stages.ci_stage_id']) }}                                  AS dim_ci_stage_sk,

    --NATURAL KEY
    ci_stages.ci_stage_id,

    --LEGACY NATURAL KEY
    ci_stages.ci_stage_id                                                                              AS dim_ci_stage_id,

    COALESCE(prep_project.dim_project_id, -1)                                                          AS dim_project_id,
    COALESCE(prep_ci_pipeline.dim_ci_pipeline_id, -1)                                                  AS dim_ci_pipeline_id,
    COALESCE(prep_namespace_plan_hist.dim_plan_id, 34)                                                 AS dim_plan_id,
    COALESCE(prep_namespace_plan_hist.dim_namespace_id, -1)                                            AS ultimate_parent_namespace_id,
    prep_date.date_id                                                                                  AS created_date_id,
    ci_stages.created_at,
    ci_stages.updated_at,
    ci_stages.ci_stage_name,
    ci_stages.ci_stage_status,
    ci_stages.lock_version,
    ci_stages.position
  FROM ci_stages
  LEFT JOIN prep_project
    ON ci_stages.project_id = prep_project.dim_project_id
  LEFT JOIN prep_namespace_plan_hist
    ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
      AND ci_stages.created_at >= prep_namespace_plan_hist.valid_from
      AND ci_stages.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
  LEFT JOIN prep_ci_pipeline
    ON ci_stages.pipeline_id = prep_ci_pipeline.dim_ci_pipeline_id
  INNER JOIN prep_date
    ON TO_DATE(ci_stages.created_at) = prep_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@chrissharp",
    created_date="2021-06-29",
    updated_date="2022-06-01"
) }}
