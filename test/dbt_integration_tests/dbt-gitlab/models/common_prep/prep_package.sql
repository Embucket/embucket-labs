{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_package_sk",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_project', 'prep_project')
]) }}

, gitlab_dotcom_packages_packages_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_packages_packages_source')}}
    {% if is_incremental() %}

      WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), renamed AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_packages_packages_source.packages_package_id']) }}     AS dim_package_sk,
      packages_package_id                                                                               AS package_id,
      -- FOREIGN KEYS
      prep_project.dim_project_id                                                                       AS dim_project_id,
      prep_namespace.dim_namespace_id                                                                   AS dim_namespace_id,
      prep_namespace.ultimate_parent_namespace_id                                                       AS ultimate_parent_namespace_id,
      dim_date.date_id                                                                                  AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                                                   AS dim_plan_id,
      gitlab_dotcom_packages_packages_source.creator_id                                                 AS creator_id,

      -- package attributes
      gitlab_dotcom_packages_packages_source.package_version                                            AS package_version,
      gitlab_dotcom_packages_packages_source.package_type                                               AS package_type,
      gitlab_dotcom_packages_packages_source.created_at                                                 AS created_at,
      gitlab_dotcom_packages_packages_source.updated_at                                                 AS updated_at

    FROM gitlab_dotcom_packages_packages_source
    LEFT JOIN prep_project
      ON gitlab_dotcom_packages_packages_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist
      ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_packages_packages_source.created_at >= dim_namespace_plan_hist.valid_from
      AND gitlab_dotcom_packages_packages_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN prep_namespace
      ON prep_project.dim_namespace_id = prep_namespace.dim_namespace_id
      AND is_currently_valid = TRUE
    INNER JOIN dim_date
      ON TO_DATE(gitlab_dotcom_packages_packages_source.created_at) = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@mpeychet_",
    updated_by="@michellecooper",
    created_date="2021-08-05",
    updated_date="2023-08-10"
) }}
