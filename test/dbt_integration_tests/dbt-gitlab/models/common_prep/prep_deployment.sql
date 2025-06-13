
{{ config({
    "materialized": "incremental",
    "unique_key": "dim_deployment_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_project', 'prep_project'),
]) }}

, gitlab_dotcom_deployments_dedupe_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_deployments_dedupe_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      gitlab_dotcom_deployments_dedupe_source.id::NUMBER                                      AS dim_deployment_id,
      gitlab_dotcom_deployments_dedupe_source.project_id::NUMBER                              AS dim_project_id,
      prep_project.ultimate_parent_namespace_id::NUMBER                                       AS ultimate_parent_namespace_id,
      dim_date.date_id::NUMBER                                                                AS created_date_id,
      IFNULL(prep_namespace_plan_hist.dim_plan_id, 34)::NUMBER                                AS dim_plan_id,
      gitlab_dotcom_deployments_dedupe_source.user_id::NUMBER                                 AS dim_user_id,
      gitlab_dotcom_deployments_dedupe_source.iid::NUMBER                                     AS deployment_internal_id,
      gitlab_dotcom_deployments_dedupe_source.environment_id::NUMBER                          AS environment_id,
      gitlab_dotcom_deployments_dedupe_source.cluster_id::NUMBER                              AS cluster_id,
      gitlab_dotcom_deployments_dedupe_source.deployable_id::NUMBER                           AS deployable_id,
      gitlab_dotcom_deployments_dedupe_source.deployable_type::VARCHAR                        AS deployable_type,
      gitlab_dotcom_deployments_dedupe_source.created_at::TIMESTAMP                           AS created_at,
      gitlab_dotcom_deployments_dedupe_source.updated_at::TIMESTAMP                           AS updated_at,
      gitlab_dotcom_deployments_dedupe_source.on_stop::VARCHAR                                AS on_stop,
      gitlab_dotcom_deployments_dedupe_source.finished_at::TIMESTAMP                          AS finished_at,
      gitlab_dotcom_deployments_dedupe_source.status::NUMBER                                  AS status_id
    FROM gitlab_dotcom_deployments_dedupe_source
    LEFT JOIN prep_project ON gitlab_dotcom_deployments_dedupe_source.project_id = prep_project.dim_project_id
    LEFT JOIN prep_namespace ON prep_project.ultimate_parent_namespace_id = prep_namespace.dim_namespace_id
        AND prep_namespace.is_currently_valid = TRUE
    LEFT JOIN prep_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_deployments_dedupe_source.created_at >= prep_namespace_plan_hist.valid_from
        AND gitlab_dotcom_deployments_dedupe_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(gitlab_dotcom_deployments_dedupe_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@chrissharp",
    created_date="2021-07-26",
    updated_date="2022-05-30"
) }}
