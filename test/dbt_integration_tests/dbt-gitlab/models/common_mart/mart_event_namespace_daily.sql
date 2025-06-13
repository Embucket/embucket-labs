{{ config(
    materialized='table',
    tags=["mnpi_exception", "product"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('fct_event_namespace_daily', 'fct_event_namespace_daily'),
    ('dim_date', 'dim_date')
    ])
}},

fact AS (

  SELECT
    {{ dbt_utils.star(from=ref('fct_event_namespace_daily'), except=["CREATED_BY",
        "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM fct_event_namespace_daily

),

fact_with_dims AS (

  SELECT
    fact.*,
    dim_namespace.namespace_type AS ultimate_parent_namespace_type,
    dim_namespace.namespace_is_internal,
    dim_namespace.namespace_creator_is_blocked,
    dim_namespace.created_at AS namespace_created_at,
    CAST(dim_namespace.created_at AS DATE) AS namespace_created_date,
    dim_date.first_day_of_month AS event_calendar_month,
    dim_date.quarter_name AS event_calendar_quarter,
    dim_date.year_actual AS event_calendar_year
  FROM fact
  LEFT JOIN dim_namespace
    ON fact.dim_ultimate_parent_namespace_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_date
    ON fact.dim_event_date_id = dim_date.date_id

)

{{ dbt_audit(
    cte_ref="fact_with_dims",
    created_by="@icooper-acp",
    updated_by="@tpoole1",
    created_date="2022-02-15",
    updated_date="2022-08-01"
) }}
