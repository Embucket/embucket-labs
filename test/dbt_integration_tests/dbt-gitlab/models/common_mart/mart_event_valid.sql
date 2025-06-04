{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_event_valid', 'fct_event_valid'),
    ('dim_user', 'dim_user'),
    ('dim_project', 'dim_project'),
    ('dim_date', 'dim_date')
    ])
}},

-- This approach may be non standard and may not work in all situations
-- Joining the namespace and project dimensions directly resulted in a large negative impact to performance
-- this may be due to the size of the dimensions and the limited number of records needed from them but that was not verified
-- the `namespace_project_map` finds the unique combination of namespace and project ids directly from the fct_event_valid table
-- and not the fct_event_valid CTE as referencing the CTE caused significant spilling and a negative impact to performance.
-- Having the `namespace_project_map` built as a temp table in a pre-hook may be more stable long term but added complexity.
-- The  `namespace_project_attrs` CTE JOINs the namespace and project dimensions and with this set up Snowflake is able to pre-filter the dimensions to only the records needed.

namespace_project_map AS (
  SELECT DISTINCT
    COALESCE(dim_ultimate_parent_namespace_id,-1) AS dim_ultimate_parent_namespace_id,
    COALESCE(dim_project_id,-1) AS dim_project_id
  FROM {{ ref('fct_event_valid') }}
),

namespace_project_attrs AS (
  SELECT
    namespace_project_map.dim_project_id,
    namespace_project_map.dim_ultimate_parent_namespace_id,
    dim_namespace.namespace_type AS ultimate_parent_namespace_type,
    dim_namespace.namespace_is_internal,
    dim_namespace.namespace_creator_is_blocked,
    dim_namespace.created_at AS namespace_created_at,
    CAST(dim_namespace.created_at AS DATE) AS namespace_created_date,
    COALESCE(dim_project.is_learn_gitlab, FALSE) AS project_is_learn_gitlab,
    COALESCE(dim_project.is_imported, FALSE) AS project_is_imported
  FROM namespace_project_map
  LEFT JOIN dim_namespace
    ON namespace_project_map.dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_project
    ON namespace_project_map.dim_project_id = dim_project.dim_project_id
),

--limit mart to rolling 24 months for performance reasons

fact AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_event_valid'), except=["CREATED_BY","NAMESPACE_CREATED_AT",
        "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_event_valid
  WHERE event_date >= DATEADD('month', -24, DATE_TRUNC('month',CURRENT_DATE))

),

fact_with_dims AS (

  SELECT
    fact.*,
    namespace_project_attrs.ultimate_parent_namespace_type,
    namespace_project_attrs.namespace_is_internal,
    namespace_project_attrs.namespace_creator_is_blocked,
    namespace_project_attrs.namespace_created_at,
    namespace_project_attrs.namespace_created_date,
    dim_user.created_at AS user_created_at,
    namespace_project_attrs.project_is_learn_gitlab,
    namespace_project_attrs.project_is_imported,
    dim_date.first_day_of_month AS event_calendar_month,
    dim_date.quarter_name AS event_calendar_quarter,
    dim_date.year_actual AS event_calendar_year
  FROM fact
  LEFT JOIN namespace_project_attrs
    ON COALESCE(fact.dim_ultimate_parent_namespace_id,-1) = namespace_project_attrs.dim_ultimate_parent_namespace_id
    AND COALESCE(fact.dim_project_id,-1) = namespace_project_attrs.dim_project_id
  LEFT JOIN dim_user
    ON fact.dim_user_sk = dim_user.dim_user_sk
  LEFT JOIN dim_date
    ON fact.dim_event_date_id = dim_date.date_id

)

{{ dbt_audit(
    cte_ref="fact_with_dims",
    created_by="@iweeks",
    updated_by="@pempey",
    created_date="2022-05-05",
    updated_date="2024-09-26"
) }}
