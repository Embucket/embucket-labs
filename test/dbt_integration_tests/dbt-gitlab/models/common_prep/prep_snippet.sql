{{ config(
    tags=["product"],
   snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_snippet_sk",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, snippet_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_snippets_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['snippet_source.snippet_id']) }}      AS dim_snippet_sk,
      snippet_source.snippet_id                                         AS snippet_id,
      snippet_source.author_id                                          AS author_id,
      IFNULL(dim_project.dim_project_id, -1)                            AS dim_project_id,
      IFNULL(dim_namespace_plan_hist.dim_namespace_id, -1)              AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                   AS dim_plan_id,
      snippet_source.snippet_type                                       AS snippet_type,
      snippet_source.visibility_level                                   AS visibility_level,
      dim_date.date_id                                                  AS created_date_id,
      snippet_source.created_at                                         AS created_at,
      snippet_source.updated_at                                         AS updated_at
    FROM snippet_source
    LEFT JOIN dim_project
      ON snippet_source.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND snippet_source.created_at >= dim_namespace_plan_hist.valid_from
        AND snippet_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(snippet_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-03-14",
    updated_date="2023-08-07"
) }}
