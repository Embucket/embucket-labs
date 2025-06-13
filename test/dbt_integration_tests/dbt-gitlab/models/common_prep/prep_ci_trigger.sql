{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_ci_trigger_id"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, ci_trigger_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ci_triggers_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      ci_trigger_source.ci_trigger_id                                   AS dim_ci_trigger_id,
      ci_trigger_source.owner_id                                        AS owner_id,
      IFNULL(dim_project.dim_project_id, -1)                            AS dim_project_id,
      IFNULL(dim_namespace_plan_hist.dim_namespace_id, -1)              AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                   AS dim_plan_id,
      dim_date.date_id                                                  AS created_date_id,
      ci_trigger_source.created_at                                      AS created_at,
      ci_trigger_source.updated_at                                      AS updated_at
    FROM ci_trigger_source
    LEFT JOIN dim_project
      ON ci_trigger_source.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND ci_trigger_source.created_at >= dim_namespace_plan_hist.valid_from
        AND ci_trigger_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(ci_trigger_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@chrissharp",
    created_date="2022-04-06",
    updated_date="2022-06-01"
) }}
