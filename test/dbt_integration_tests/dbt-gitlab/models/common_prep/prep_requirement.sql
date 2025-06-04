{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_requirement_sk",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_project', 'prep_project'),
]) }}

, gitlab_dotcom_requirements_dedupe_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_requirements_dedupe_source') }}
    {% if is_incremental() %}

    WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_requirements_dedupe_source.id']) }}       AS dim_requirement_sk,
      gitlab_dotcom_requirements_dedupe_source.id::NUMBER                                  AS requirement_id,
      gitlab_dotcom_requirements_dedupe_source.project_id::NUMBER                          AS dim_project_id,
      prep_project.ultimate_parent_namespace_id::NUMBER                                    AS ultimate_parent_namespace_id,
      dim_date.date_id::NUMBER                                                             AS created_date_id,
      IFNULL(prep_namespace_plan_hist.dim_plan_id, 34)::NUMBER                             AS dim_plan_id,
      gitlab_dotcom_requirements_dedupe_source.author_id::NUMBER                           AS author_id,
      iid::NUMBER                                                                          AS requirement_internal_id,
      CASE
        WHEN state::VARCHAR = '1' THEN 'opened'
        WHEN state::VARCHAR = '2' THEN 'archived'
        ELSE state::VARCHAR
      END                                                                                  AS requirement_state,
      gitlab_dotcom_requirements_dedupe_source.created_at::TIMESTAMP                       AS created_at,
      gitlab_dotcom_requirements_dedupe_source.updated_at::TIMESTAMP                       AS updated_at
    FROM gitlab_dotcom_requirements_dedupe_source
    LEFT JOIN prep_project ON gitlab_dotcom_requirements_dedupe_source.project_id = prep_project.dim_project_id
    LEFT JOIN prep_namespace ON prep_project.ultimate_parent_namespace_id = prep_namespace.dim_namespace_id
        AND prep_namespace.is_currently_valid = TRUE
    LEFT JOIN prep_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = prep_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_requirements_dedupe_source.created_at >= prep_namespace_plan_hist.valid_from
        AND gitlab_dotcom_requirements_dedupe_source.created_at < COALESCE(prep_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN dim_date ON TO_DATE(gitlab_dotcom_requirements_dedupe_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-08-10",
    updated_date="2023-08-10"
) }}
