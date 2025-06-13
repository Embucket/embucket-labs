{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_integration_sk",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_project', 'dim_project'),
]) }}

, integration_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_integrations_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['integration_source.integration_id']) }}      AS dim_integration_sk,
      integration_source.integration_id                                         AS integration_id,
      IFNULL(dim_project.dim_project_id, -1)                                    AS dim_project_id,
      IFNULL(dim_project.ultimate_parent_namespace_id, -1)                      AS ultimate_parent_namespace_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                           AS dim_plan_id,
      dim_date.date_id                                                          AS created_date_id,
      integration_source.is_active                                              AS is_active,
      integration_source.integration_type                                       AS integration_type,
      integration_source.integration_category                                   AS integration_category,
      integration_source.created_at::TIMESTAMP                                  AS created_at,
      integration_source.updated_at::TIMESTAMP                                  AS updated_at
    FROM  integration_source
    LEFT JOIN dim_project
      ON  integration_source.project_id = dim_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist
      ON dim_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND  integration_source.created_at >= dim_namespace_plan_hist.valid_from
      AND  integration_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(integration_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-03-28",
    updated_date="2023-08-16"
) }}
